/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.hbase

import java.io.IOException
import java.lang.reflect.UndeclaredThrowableException
import java.security.{Principal, PrivilegedAction, PrivilegedExceptionAction}
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}
import javax.security.auth.Subject

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.sql.execution.datasources.hbase.HBaseCredentialsManager.TokenRemovingUser
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

final class HBaseCredentialsManager private() extends Logging {
  private class TokenInfo(
      val expireTime: Long,
      val issueTime: Long,
      val conf: Configuration,
      val token: Token[_ <: TokenIdentifier]) {

    val isTokenInfoExpired: Boolean = {
      System.currentTimeMillis() >=
        ((expireTime - issueTime) * HBaseCredentialsManager.expireTimeFraction + issueTime).toLong
    }

    val refreshTime: Long = {
      require(expireTime > issueTime,
        s"Token expire time $expireTime is smaller than issue time $issueTime")

      // the expected expire time would be 60% of real expire time, to avoid long running task
      // failure.
      ((expireTime - issueTime) * HBaseCredentialsManager.refreshTimeFraction + issueTime).toLong
    }
  }
  private val tokensMap = new mutable.HashMap[String, TokenInfo]

  // We assume token expiration time should be no less than 10 minutes.
  private val nextRefresh = TimeUnit.MINUTES.toMillis(HBaseCredentialsManager.refreshDurationMins)

  private val tokenUpdater =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("HBase Tokens Refresh Thread"))

  private val tokenUpdateRunnable = new Runnable {
    override def run(): Unit = Utils.logUncaughtExceptions(updateTokensIfRequired())
  }

  tokenUpdater.scheduleAtFixedRate(
    tokenUpdateRunnable, nextRefresh, nextRefresh, TimeUnit.MILLISECONDS)

  /**
   * Get HBase credential from specified cluster name.
   */
  def getCredentialsForCluster(conf: Configuration): Credentials = {
    val credentials = new Credentials()
    val identifier = clusterIdentifier(conf)

    val tokenOpt = this.synchronized {
      tokensMap.get(identifier)
    }

    // If token is existed and not expired, directly return the Credentials with tokens added in.
    if (tokenOpt.isDefined && !tokenOpt.get.isTokenInfoExpired) {
      credentials.addToken(tokenOpt.get.token.getService, tokenOpt.get.token)
      logInfo(s"Use existing token for on-demand cluster $identifier")
    } else {

      logInfo(s"getCredentialsForCluster: Obtaining new token for cluster $identifier")

      // Acquire a new token if not existed or old one is expired.
      val tokenInfo = getNewToken(conf, tokenOpt.map(_.token) )
      this.synchronized {
        tokensMap.put(identifier, tokenInfo)
      }

      logInfo(s"getCredentialsForCluster: Obtained new token with expiration time" +
        s" ${new Date(tokenInfo.expireTime)} and refresh time ${new Date(tokenInfo.refreshTime)} " +
        s"for cluster $identifier")

      credentials.addToken(tokenInfo.token.getService, tokenInfo.token)
    }

    credentials
  }

  def isCredentialsRequired(conf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled &&
      conf.get("hbase.security.authentication") == "kerberos"
  }

  private def updateTokensIfRequired(): Unit = {
    val currTime = System.currentTimeMillis()

    // Filter out all the tokens should be re-issued.
    val tokensShouldUpdate = this.synchronized {
      tokensMap.filter { case (_, tokenInfo) => tokenInfo.refreshTime <= currTime }
    }

    if (tokensShouldUpdate.isEmpty) {
      logInfo("Refresh Thread: No tokens require update")
    } else {
      // Update all the expect to be expired tokens
      val updatedTokens = tokensShouldUpdate.map { case (cluster, tokenInfo) =>
        logInfo(s"Refresh Thread: Update token for cluster $cluster")

        val token = {
          try {
            val tok = getNewToken(tokenInfo.conf, Option(tokenInfo.token) )
            logInfo(s"Refresh Thread: Successfully obtained token for cluster $cluster")
            tok
          } catch {
            case NonFatal(ex) =>
              logWarning(s"Refresh Thread: Unable to fetch tokens from HBase cluster $cluster", ex)
              null
          }
        }
        (cluster, token)
      }.filter(null != _._2)

      this.synchronized {
        updatedTokens.foreach { kv => tokensMap.put(kv._1, kv._2) }
      }
    }
  }

  /*
   * This method and the infrastructure in HBaseCredentialManager object are trying to workaround an
   * issue with hbase token acquisition.
   * HBase allows token acquisition only if it detects that the incoming user (at server) is
   * kerberos authorized. Since we need to access hbase at driver, we end up adding the acquired
   * tokens to current user credentials - which allows us hbase acces. Unfortunately, when we do
   * token renewal with the same user, hbase will now reject our request - since it sees that the
   * incoming user is authorized by token.
   * Since getNewToken can be invoked in multiple threads, while hbase access might be happening in
   * parallel, we cannot "remove tokens from ugi.currentUser for the cluster just for token
   * acquisition and add new tokens once acquired".
   *
   * In order to workaround this issue, we directly access the underlying Subject in UGI, create a
   * copy of it, modify the copy's private credentials to remove existing tokens for hbase for
   * the cluster in question (if we had previously added tokens for it - this also means
   * that the cluster id MUST uniquely identify the cluster), create a new ugi our of this and
   * use this new ugi to create a hbase User - for use with connection and token acquisition.
   * This new user will have everything in common with ugi.currentUser - except for the token we
   * removed for the cluster.
   *
   * This will cause the token acquisition to behave as it did for the first time we acquired token for
   * cluster - and resulting in working around the hbase token acquisition constraint.
   */
  private def getNewToken(conf: Configuration,
      currentTokenOpt: Option[Token[_ <: TokenIdentifier]]): TokenInfo = {
    val token = {
      if (currentTokenOpt.isDefined) {
        val modifiedUser = new TokenRemovingUser(currentTokenOpt.get)
        var connection: Connection = null
        try {
          connection = ConnectionFactory.createConnection(conf, modifiedUser)
          TokenUtil.obtainToken(connection, modifiedUser)
        } finally {
          if (null != connection) {
            try { connection.close() } catch { case ex: Exception => /* ignore */ }
          }
        }
      } else {
        TokenUtil.obtainToken(conf)
      }
    }

    val tokenIdentifier = token.decodeIdentifier()
    val expireTime = tokenIdentifier.getExpirationDate
    val issueTime = tokenIdentifier.getIssueDate
    new TokenInfo(expireTime, issueTime, conf, token)
  }

  private def clusterIdentifier(conf: Configuration): String = {
    require(conf.get("zookeeper.znode.parent") != null &&
      conf.get("hbase.zookeeper.quorum") != null &&
      conf.get("hbase.zookeeper.property.clientPort") != null)

    conf.get("zookeeper.znode.parent") + "-"
      conf.get("hbase.zookeeper.quorum") + "-"
      conf.get("hbase.zookeeper.property.clientPort")
  }
}

object HBaseCredentialsManager extends Logging {

  private val expireTimeFraction = 0.95
  private val refreshTimeFraction = 0.6
  private val refreshDurationMins = 10

  lazy val manager = new HBaseCredentialsManager

  // See HBaseCredentialsManager#getNewToken for explaination about why we need everything
  // below this comment


  private lazy val getSubjectMethod = {
    val method = classOf[UserGroupInformation].getDeclaredMethod("getSubject")
    method.setAccessible(true)
    method
  }

  private lazy val ugiConstructor = {
    val constructor = classOf[UserGroupInformation].getDeclaredConstructor(classOf[Subject])
    constructor.setAccessible(true)
    constructor
  }

  private def fetchSubject(ugi: UserGroupInformation): Subject = {
    getSubjectMethod.invoke(ugi).asInstanceOf[Subject]
  }

  private def createTokenRemovedUGI(tokenToRemove: Token[_ <: TokenIdentifier]): UserGroupInformation = {
    val currentUgi = UserGroupInformation.getCurrentUser
    currentUgi.synchronized {
      val subject = fetchSubject(currentUgi)

      val principals = new java.util.LinkedHashSet[Principal](subject.getPrincipals)
      val publicCreds = new java.util.LinkedHashSet[java.lang.Object](subject.getPublicCredentials)
      val privateCreds = new java.util.LinkedHashSet[java.lang.Object](subject.getPrivateCredentials)

      val privateCredsIter = privateCreds.iterator()
      var removed = false
      val newCredentialsList = new mutable.ArrayBuffer[Credentials]()
      val tokenToRemoveSet = new java.util.HashSet[Token[_ <: TokenIdentifier]]()
      tokenToRemoveSet.add(tokenToRemove)

      while (privateCredsIter.hasNext) {
        val entry = privateCredsIter.next()
        entry match {
          case creds: Credentials =>
            val newCreds = new Credentials()
            newCreds.addAll(creds)


            if (newCreds.getAllTokens.removeAll(tokenToRemoveSet)) {
              logInfo("Removed token from credential. token = " + tokenToRemove)
              removed = true
            }
            privateCredsIter.remove()
            newCredentialsList += newCreds
          case _ =>
        }
      }

      // The part below does not required to be in synchronized block.
      // But pulling it out is cumbersome, and the code is not expensive.

      if (!removed) {
        logInfo("Unable to find token from privateCreds. tokenToRemove = " + tokenToRemove)
      }

      val subjectCopy = new Subject(false, principals, publicCreds, privateCreds)

      val ugi = ugiConstructor.newInstance(subjectCopy)
      for (cred <- newCredentialsList) ugi.addCredentials(cred)

      ugi
    }
  }

  // Essentially a copy of SecureHadoopUser
  private class TokenRemovingUser(tokenToRemove: Token[_ <: TokenIdentifier]) extends User {

    private var shortName: String = _

    this.ugi = createTokenRemovedUGI(tokenToRemove)

    override def runAs[T](privilegedAction: PrivilegedAction[T]): T = {
      ugi.doAs(privilegedAction)
    }

    override def runAs[T](privilegedExceptionAction: PrivilegedExceptionAction[T]): T = {
      ugi.doAs(privilegedExceptionAction)
    }

    override def getShortName: String = {
      if(null == shortName) {
        try {
          shortName = ugi.getShortUserName
        } catch {
          case ex: Exception =>
            throw new RuntimeException("Unexpected error getting user short name", ex)
        }
      }
      shortName
    }

    override def obtainAuthTokenForJob(conf: Configuration, job: Job): Unit = {
      try {
        TokenUtil.obtainTokenForJob(conf, ugi, job)
      } catch {
        case ex: IOException => throw ex
        case ex: InterruptedException => throw ex
        case ex: RuntimeException => throw ex
        case ex: Exception => throw new UndeclaredThrowableException(
            ex, "Unexpected error calling TokenUtil.obtainAndCacheToken()");
      }
    }

    override def obtainAuthTokenForJob(jobConf: JobConf): Unit = {
      try {
        TokenUtil.obtainTokenForJob(jobConf, ugi)
      } catch {
        case ex: IOException => throw ex
        case ex: InterruptedException => throw ex
        case ex: RuntimeException => throw ex
        case ex: Exception => throw new UndeclaredThrowableException(
          ex, "Unexpected error calling TokenUtil.obtainAndCacheToken()");
      }
    }
  }
}
