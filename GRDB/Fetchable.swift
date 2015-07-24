//
// GRDB.swift
// https://github.com/groue/GRDB.swift
// Copyright (c) 2015 Gwendal Roué
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.


public protocol RowFetchable {
    typealias FetchedType
    static func fetch(db: Database, _ sql: String, arguments: QueryArguments?) -> AnySequence<FetchedType>
    static func fetchAll(db: Database, _ sql: String, arguments: QueryArguments?) -> [FetchedType]
    static func fetchOne(db: Database, _ sql: String, arguments: QueryArguments?) -> FetchedType?
}

public extension RowFetchable where Self : RowModel, FetchedType == Self {
    
    /**
    Fetches a lazy sequence of RowModels.

        let persons = Person.fetch(db, "SELECT * FROM persons")

    - parameter type:     The type of fetched row models. It must be a subclass
                          of RowModel.
    - parameter sql:      An SQL query.
    - parameter arguments: Optional query arguments.
    
    - returns: A lazy sequence of row models.
    */
    static func fetch(db: Database, _ sql: String, arguments: QueryArguments? = nil) -> AnySequence<FetchedType> {
        return db.selectStatement(sql).fetch(FetchedType.self, arguments: arguments)
    }
    
    /**
    Fetches an array of RowModels.

        let persons = db.fetchAll(Person.self, "SELECT * FROM persons")

    - parameter type:     The type of fetched row models. It must be a subclass
                          of RowModel.
    - parameter sql:      An SQL query.
    - parameter arguments: Optional query arguments.
    
    - returns: An array of row models.
    */
    static func fetchAll(db: Database, _ sql: String, arguments: QueryArguments? = nil) -> [FetchedType] {
        return Array(fetch(db, sql, arguments: arguments))
    }
    
    /**
    Fetches a single RowModel.

        let person = Person.fetchOne(db, "SELECT * FROM persons")

    - parameter type:     The type of fetched row model. It must be a subclass
                          of RowModel.
    - parameter sql:      An SQL query.
    - parameter arguments: Optional query arguments.
    
    - returns: An optional row model.
    */
    static func fetchOne(db: Database, _ sql: String, arguments: QueryArguments? = nil) -> FetchedType? {
        return fetch(db, sql, arguments: arguments).generate().next()
    }


    /**
    Fetches a single RowModel by primary key.

        let person = Person.fetchOne(db, primaryKey: 123)

    - parameter type:       The type of fetched row model. It must be a subclass
                            of RowModel.
    - parameter primaryKey: A value.
    - returns: An optional row model.
    */
    static func fetchOne(db: Database, primaryKey: DatabaseValueConvertible?) -> FetchedType? {
        guard let primaryKey = primaryKey else {
            return nil
        }
        
        // Select methods crash when there is an issue
        guard let table = FetchedType.databaseTable else {
            fatalError("Nil Table returned from \(FetchedType.self).databaseTable")
        }
        
        guard let tablePrimaryKey = table.primaryKey else {
            fatalError("Nil Primary Key in \(FetchedType.self).databaseTable")
        }
        
        let sql: String
        switch tablePrimaryKey {
        case .RowID(let column):
            sql = "SELECT * FROM \(table.name.quotedDatabaseIdentifier) WHERE \(column.quotedDatabaseIdentifier) = ?"
        case .Column(let column):
            sql = "SELECT * FROM \(table.name.quotedDatabaseIdentifier) WHERE \(column.quotedDatabaseIdentifier) = ?"
        case .Columns(let columns):
            guard columns.count == 1 else {
                fatalError("Primary key columns count mismatch in \(FetchedType.self).databaseTable")
            }
            sql = "SELECT * FROM \(table.name.quotedDatabaseIdentifier) WHERE \(columns.first!.quotedDatabaseIdentifier) = ?"
        }
        
        return fetchOne(db, sql, arguments: [primaryKey])
    }
    
    /**
    Fetches a single RowModel given a key.

        let person = Person.fetchOne(db, key: ["name": Arthur"])

    - parameter type: The type of fetched row model. It must be a subclass of
                      RowModel.
    - parameter key:  A dictionary of values.
    - returns: An optional row model.
    */
    static func fetchOne(db: Database, key dictionary: [String: DatabaseValueConvertible?]?) -> FetchedType? {
        guard let dictionary = dictionary else {
            return nil
        }
        
        // Select methods crash when there is an issue
        guard let table = FetchedType.databaseTable else {
            fatalError("Nil Table returned from \(FetchedType.self).databaseTable")
        }
        
        let whereSQL = " AND ".join(dictionary.keys.map { column in "\(column.quotedDatabaseIdentifier)=?" })
        let sql = "SELECT * FROM \(table.name.quotedDatabaseIdentifier) WHERE \(whereSQL)"
        return fetchOne(db, sql, arguments: QueryArguments(dictionary.values))
    }
}

public protocol FetchableValue {
    typealias FetchedType
    static func fetch(db: Database, _ sql: String, arguments: QueryArguments?) -> AnySequence<FetchedType?>
    static func fetchAll(db: Database, _ sql: String, arguments: QueryArguments?) -> [FetchedType?]
    static func fetchOne(db: Database, _ sql: String, arguments: QueryArguments?) -> FetchedType?
}

public extension FetchableValue where Self : DatabaseValueConvertible, FetchedType == Self {
    /**
    Fetches a lazy sequence of values.

        let names = String.fetch(db, "SELECT name FROM ...")

    - parameter type:      The type of fetched values. It must adopt
                           DatabaseValueConvertible.
    - parameter sql:       An SQL query.
    - parameter arguments: Optional query arguments.
    - returns: A lazy sequence of values.
    */
    static func fetch(db: Database, _ sql: String, arguments: QueryArguments? = nil) -> AnySequence<FetchedType?> {
        return db.selectStatement(sql).fetch(FetchedType.self, arguments: arguments)
    }
    
    /**
    Fetches an array of values.

        let names = String.fetchAll(db, "SELECT name FROM ...")

    - parameter type:      The type of fetched values. It must adopt
                           DatabaseValueConvertible.
    - parameter sql:       An SQL query.
    - parameter arguments: Optional query arguments.
    - returns: An array of values.
    */
    static func fetchAll(db: Database, _ sql: String, arguments: QueryArguments? = nil) -> [FetchedType?] {
        return Array(fetch(db, sql, arguments: arguments))
    }
    
    /**
    Fetches a single value.

        let name = String.fetchOne(db, "SELECT name FROM ...")

    - parameter type:      The type of fetched values. It must adopt
                           DatabaseValueConvertible.
    - parameter sql:       An SQL query.
    - parameter arguments: Optional query arguments.
    - returns: An optional value.
    */
    static func fetchOne(db: Database, _ sql: String, arguments: QueryArguments? = nil) -> FetchedType? {
        if let value = fetch(db, sql, arguments: arguments).generate().next() {
            return value
        } else {
            return nil
        }
    }
}

