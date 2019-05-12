package com.sergeyolontsev.azure.cosmosdb.scala.basic

import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel, Database, DocumentClientException, ResourceResponse}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import rx.Observable

import scala.util.{Failure, Success, Try}

object Main {

  private val databaseName = "MyTestDb"
  private val cosmosdbUri = ""
  private val cosmosdbKey = ""

  private val client = new AsyncDocumentClient.Builder()
    .withServiceEndpoint(cosmosdbUri)
    .withMasterKeyOrResourceToken(cosmosdbKey)
    .withConnectionPolicy(ConnectionPolicy.GetDefault())
    .withConsistencyLevel(ConsistencyLevel.Session)
    .build()

  private def createDatabaseIfNotExists(): Unit = {
    val dbReadObs = client.readDatabase(s"/dbs/$databaseName", null)

    val dbExistObs = dbReadObs.doOnNext(
      (x: ResourceResponse[Database]) => println(s"Database $databaseName exists. Self-Link: ${x.getResource.getSelfLink}")
    )
      .onErrorResumeNext(
        (e: Throwable) => {
          e match {
            case x: DocumentClientException if x.getStatusCode == 404 => {
              val db = (new Database())
              db.setId(databaseName)

              client.createDatabase(db, null)
            }
            case _ => Observable.error(e)
          }
        }
      )

    dbExistObs.toCompletable.await()
  }

  def main(args: Array[String]): Unit = {
    // Creating database
    val resultCreateDb = Try(createDatabaseIfNotExists())
    resultCreateDb match {
      case Success(_) => println(s"Database $databaseName successfully created.")
      case Failure(exception) => print(s"Error while creating database: ${exception.getMessage}")
    }

    // Creating collection

    // Adding documents

    // Reading documents

    // Updating document

    // Removing document

    client.close()
  }
}
