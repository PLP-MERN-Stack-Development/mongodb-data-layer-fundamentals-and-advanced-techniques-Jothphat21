// queries.js - MongoDB Queries for PLP Bookstore Assignment

const { MongoClient } = require("mongodb");

const uri = "mongodb://localhost:27017";
const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const books = db.collection(collectionName);


    // Task 2: Basic CRUD Ops

    // 1. Find all books in a specific genre (e.g., Fiction)
    console.log("\nBooks in Fiction genre:");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    // 2. Find books published after a certain year (e.g., 1940)
    console.log("\nBooks published after 2000:");
    console.log(await books.find({ published_year: { $gt: 1940 } }).toArray());

    // 3. Find books by a specific author (e.g., George Orwell)
    console.log("\nBooks by George Orwell:");
    console.log(await books.find({ author: "George Orwell" }).toArray());

    // 4. Update the price of a specific book
    console.log("\nUpdating price of '1984'...");
    await books.updateOne({ title: "1984" }, { $set: { price: 15.99 } });
    console.log("Updated book:", await books.findOne({ title: "1984" }));

    // 5. Delete a book by its title
    console.log("\nDeleting book 'Moby Dick'...");
    await books.deleteOne({ title: "Moby Dick" });
    console.log("Deleted. Checking:", await books.findOne({ title: "Moby Dick" }));


    //Task 3: Advanced Queries

    // 1. Books that are in stock and published after 2010
    console.log("\nBooks in stock & published after 1930:");
    console.log(
      await books
        .find({ in_stock: true, published_year: { $gt: 1930 } })
        .toArray()
    );

    // 2. Projection: return only title, author, and price
    console.log("\nProjection (title, author, price only):");
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1 } }).toArray());

    // 3. Sort by price ascending
    console.log("\nBooks sorted by price (ascending):");
    console.log(await books.find().sort({ price: 1 }).toArray())

    // 4. Sort by price descending
    console.log("\nBooks sorted by price (descending):");
    console.log(await books.find().sort({ price: -1 }).toArray());

     // 5. Pagination (5 per page)
    console.log("\nPagination (Page 1, 5 books):");
    console.log(await books.find().skip(0).limit(5).toArray());

    console.log("\nPagination (Page 2, 5 books):");
    console.log(await books.find().skip(5).limit(5).toArray());

    //Task 4: Aggregation Pipeline

    // 1. Average price of books by genre
    console.log("\nAverage price by genre:");
    console.log(
      await books.aggregate([
        { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
      ]).toArray()
    );

    // 2. Author with the most books
    console.log("\nAuthor with the most books:");
    console.log(
      await books.aggregate([
        { $group: { _id: "$author", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 1 }
      ]).toArray()
    );

    // 3. Group books by publication decade
    console.log("\nBooks grouped by publication decade:");
    console.log(
      await books.aggregate([
        {
          $group: {
            _id: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] },
            count: { $sum: 1 }
         }
        },
        { $sort: { _id: 1 } }
      ]).toArray()
    );

    //Task 5: Indexing

    // 1. Create index on title
    console.log("\nCreating index on title...");
    await books.createIndex({ title: 1 });

    // 2. Create compound index on author and published_year
    console.log("\nCreating compound index (author + published_year)...");
    await books.createIndex({ author: 1, published_year: -1 });

    // 3. Use explain() to show performance with index
    console.log("\nExplain query using title index:");
    console.log(
      await books.find({ title: "1984" }).explain("executionStats")
    );


  }catch (err) {
    console.error("Error running queries:", err);
  } finally {
    await client.close();
    console.log("\nConnection closed");
  }
}


runQueries().catch(console.error);