package com.sergeyolontsev.azure.cosmosdb.scala.basic

import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel, DataType, Database, DocumentClientException, DocumentCollection, IncludedPath, Index, IndexingPolicy, PartitionKeyDefinition, RequestOptions, ResourceResponse}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import rx.Observable

import scala.util.{Failure, Success, Try}

object Main {

  private val databaseName = "MyTestDb"
  private val cosmosdbUri = ""
  private val cosmosdbKey = ""

  private val collectionName = "MyCollection"
  private val partitionKeyPath = "/type"
  private val throughPut = 400

  private val client = new AsyncDocumentClient.Builder()
    .withServiceEndpoint(cosmosdbUri)
    .withMasterKeyOrResourceToken(cosmosdbKey)
    .withConnectionPolicy(ConnectionPolicy.GetDefault())
    .withConsistencyLevel(ConsistencyLevel.Session)
    .build()

  private def createDatabaseIfNotExists(): Unit = {
    val dbReadObs = client.readDatabase(s"/dbs/$databaseName", null)

    val dbOperationObs = dbReadObs
      .doOnNext(
        (x: ResourceResponse[Database]) => println(s"Database $databaseName already exists. Self-Link: ${x.getResource.getSelfLink}")
      )
      .onErrorResumeNext(
        (e: Throwable) =>
          e match {
            case x: DocumentClientException if x.getStatusCode == 404 => {
              val db = (new Database())
              db.setId(databaseName)

              client.createDatabase(db, null)
            }
            case _ => Observable.error(e)
          }
      )

    dbOperationObs.toCompletable.await()
  }

  private def createCollection(): Unit = {
    // Creating collection defenition
    val collectionDefinition = new DocumentCollection
    collectionDefinition.setId(collectionName)

    val partitionKeyDefinition = new PartitionKeyDefinition
    val paths = new java.util.ArrayList[String]
    paths.add(partitionKeyPath)
    partitionKeyDefinition.setPaths(paths)
    collectionDefinition.setPartitionKey(partitionKeyDefinition)

    // Set indexing policy to be range for string and number

    val indexingPolicy = new IndexingPolicy
    val includedPaths = new java.util.ArrayList[IncludedPath]
    val includedPath = new IncludedPath
    includedPath.setPath("/*")
    val indexes = new java.util.ArrayList[Index]
    val stringIndex = Index.Range(DataType.String)
    stringIndex.set("precision", -1)
    indexes.add(stringIndex)

    val numberIndex = Index.Range(DataType.Number)
    numberIndex.set("precision", -1)
    indexes.add(numberIndex)
    includedPath.setIndexes(indexes)
    includedPaths.add(includedPath)
    indexingPolicy.setIncludedPaths(includedPaths)
    collectionDefinition.setIndexingPolicy(indexingPolicy)

    //Create collection
    val multiPartitionRequestOptions = new RequestOptions
    multiPartitionRequestOptions.setOfferThroughput(throughPut)
    val databaseLink = s"/dbs/$databaseName"
    val collectionLink = s"/dbs/$databaseName/colls/$collectionName"

    //val createCollectionObs = client.createCollection(databaseLink, collectionDefinition, multiPartitionRequestOptions)
    val colReadObs = client.readCollection(collectionLink, null)

    val colOperationObs = colReadObs
      .doOnNext(
        (x: ResourceResponse[DocumentCollection]) => println(s"Collection $collectionName already exists. Self-Link: ${x.getResource.getSelfLink}")
      )
      .onErrorResumeNext(
        (e: Throwable) =>
          e match {
            case x: DocumentClientException if x.getStatusCode == 404 => {
              client.createCollection(databaseLink, collectionDefinition, multiPartitionRequestOptions)
            }
            case _ => Observable.error(e)
          }
      )

    colOperationObs.toCompletable.await()
  }

  def main(args: Array[String]): Unit = {
    // Creating database
    val resultCreateDb = Try(createDatabaseIfNotExists())
    resultCreateDb match {
      case Success(_) => println(s"Operation completed successfully: creating database $databaseName if not exists.")
      case Failure(exception) => print(s"Error while creating database: ${exception.getMessage}")
    }

    // Creating collection
    val resultCreateCollection = Try(createCollection())
    resultCreateCollection match {
      case Success(_) => println(s"Operation completed successfully: creating collection $collectionName if not exists.")
      case Failure(exception) => print(s"Error while creating collection: ${exception.getMessage}")
    }

    // Adding documents

    // Reading documents

    // Updating document

    // Removing document

    client.close()
  }
}
