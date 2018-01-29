'use strict';
//Dependencies
const Alexa = require('alexa-sdk');
const sql = require('mssql');
const requests = require('requests');
const Graph = require('node-dijkstra');
const https = require('https');
const apiai = require('apiai');

// Amazon and DialogFlow API tokens
const APP_ID = 'amzn1.ask.skill.e40ee34f-e289-4920-8f14-74500c9de490';
const AIAPI_DEV_ACCESS_TOKEN = 'ab74b3a10ba84131b712203b6304e877'; 

const app = apiai(AIAPI_DEV_ACCESS_TOKEN);
const SKILL_NAME = 'SpeaQL';

// Default Alexa intent messages
const HELP_MESSAGE = 'You can ask me to query the database, or you can tell me to stop. What can I help you with?';
const HELP_REPROMPT = 'What can I help you with?';
const STOP_MESSAGE = 'Goodbye!';

// Alexa handler
exports.handler = function(event, context, callback) {
    var alexa = Alexa.handler(event, context);
    alexa.appId = APP_ID;
    alexa.registerHandlers(handlers);
    alexa.execute();
};

// List of Alexa intents, standard plus custom QueryDatabaseIntent
const handlers = {
    // Pass LaunchRequest intent to QueryDatabase intent
    'LaunchRequest': function () {
        this.emit('QueryDatabaseIntent');
    },
    // Custom intent
    'QueryDatabaseIntent': function () {
        // queryString is string from Alexa slot (i.e. question asked to Alexa)
        const queryString = this.event.request.intent.slots.Query.value;
        // Send queryString to DialogFlow
        var request = app.textRequest(queryString, {
            sessionId : '1'
        });

        // Get response from Dialogflow, query SQL DB, send back to Alexa
        var self = this;
        request.on('response', function(apiResponse) {
            
            // Get JSON from DialogFlow and shorten
            var JSON_returned = apiResponse.result;
            var values = JSON_returned.parameters;

            // Format table names to full name
            function tableLongName(tableShortName) {
                var table = tableShortName;
                if(table == "PMPD") { table = "ProductModelProductDescription"; }
                if(table == "P") { table = "Product"; }
                if(table == "PM") { table = "ProductModel"; }
                if(table == "PD") { table = "ProductDescription"; }
                if(table == "PC") { table = "ProductCategory"; }
                if(table == "SOH") { table = "SalesOrderHeader"; }
                if(table == "SOD") { table = "SalesOrderDetail"; }
                if(table == "A") { table = "Address"; }
                if(table == "C") { table = "Customer"; }
                if(table == "CA") { table = "CustomerAddress"; }
                return table;
            }
            // Column names come from DialogFlow as T.Column, format
            function parseColumnName(columnLongName) {
                var columnNameArr = values.column_name.split('.');
                return columnNameArr;
            }
            function getTableNameforColumn(columnLongName) {
                var columnNameArr = parseColumnName(columnLongName);
                var tableName = columnNameArr[1];
                tableName = tableLongName(tableName);
                return tableName;
            }
            function getColumnNameforColumn(columnLongName) {
                var columnNameArr = parseColumnName(columnLongName);
                var columnName = columnNameArr[2];
                return columnName;
            }

            // sqlVal contains SQL function code from DialogFlow
            // tableVal has value of table name if table is returned as value from DF
            // columnVal has value of column name if column is returned as value from DF
            var sqlVal, tableVal, columnVal; 
            // To fill with array of objects that contain data values from DF
            var dataArr = [];
            // Tables from DialogFlow, no duplicates, use to find shortest table join path
            var mentions = [];

            // Build array dataArr of objects and assign 
            Object.keys(values).forEach(function(key) {
                // SQL function from DF
                if(key == "sql_statement") { sqlVal = values[key]; }
                // Table from DF
                else if(key == "table_name" && values[key] != "") { 
                    tableVal = tableLongName(values[key]);
                    mentions.push(tableVal);
                }
                // Column from DF
                else if(key == "column_name" && values[key] != "") { 
                    columnVal = values[key];
                    tableVal = getTableNameforColumn(columnVal);
                    mentions.push(tableVal);
                    columnVal = getColumnNameforColumn(columnVal);
                    console.log(columnVal);
                }
                // Ignore geo-city built in entity from DF 
                else if(key == "geo-city") { }
                // Data from DF
                else {
                    if(values[key] != "") {
                    var str_arr = key.split("-");
                    var table = tableLongName(str_arr[0]);
                   
                    var obj = {
                       "Table": table,
                       "Column": str_arr[1],
                       "Data": values[key]
                    }
                    dataArr.push(obj);
                   
                    console.log(dataArr[0]);
                }
               }
            });

            // All tables to query, no duplicates
            dataArr.forEach(function (entry) {
                if(!mentions.includes(entry.Table)) {
                    mentions.push(entry.Table);
                }
            });

            // determine how many paths we need to calculate through the database
            var pairs = mentions.reduce((acc, v, i) =>
                acc.concat(mentions.slice(i + 1).map(w => [v, w])),
                []);

            // define the relationships between tables in the database
            var relationships = [
                {
                    pair: ['Product', 'ProductModel'],
                    relation: 'Product.ProductModelId = ProductModel.ProductModelId'
                },
                {
                    pair: ['Product', 'ProductCategory'],
                    relation: 'Product.ProductCategoryId = ProductCategory.ProductCategoryId'
                },
                {
                    pair: ['Product', 'SalesOrderDetail'],
                    relation: 'SalesOrderDetail.ProductId = Product.ProductId'
                },
                {
                    pair: ['SalesOrderHeader', 'SalesOrderDetail'],
                    relation: 'SalesOrderHeader.SalesOrderId = SalesOrderDetail.SalesOrderId'
                },
                {
                    pair: ['SalesOrderHeader', 'Customer'],
                    relation: 'SalesOrderHeader.CustomerId = Customer.CustomerId'
                },
                {
                    pair: ['ProductModelProductDescription', 'ProductDescription'],
                    relation: 'ProductModelProductDescription.ProductDescriptionId = ProductDescription.ProductDescriptionId'
                },
                {
                    pair: ['Address', 'SalesOrderHeader'],
                    relation: 'SalesOrderHeader.ShipToAddressId = Address.AddressId'
                },
                {
                    pair: ['CustomerAddress', 'Address'],
                    relation: 'CustomerAddress.AddressId = Address.AddressId'
                },
                {
                    pair: ['CustomerAddress', 'Customer'],
                    relation: 'CustomerAddress.CustomerId = Address.CustomerId'
                },
                {
                    pair: ['ProductModelProductDescription', 'ProductModel'],
                    relation: 'ProductModelProductDescription.ProductModelId = ProductModel.ProductModelId'
                }
            ];

            // construct a graph
            var route = new Graph();
            route.addNode('Product', {
                'ProductModel': 1,
                'ProductCategory': 1,
                'SalesOrderDetail': 1
            });

            route.addNode('ProductModel', { 'Product': 1 });
            route.addNode('ProductCategory', { 'Product': 1 });
            route.addNode('SalesOrderDetail', {
                'Product': 1,
                'SalesOrderHeader': 1
            });

            route.addNode('SalesOrderHeader', {
                'SalesOrderDetail': 1,
                'Customer': 1,
                'Address' : 1
            });

            route.addNode('Customer', {
                'SalesOrderHeader': 1,
                'CustomerAddress' : 1
            });

            route.addNode('ProductModelProductDescription', {
                'ProductModel': 1,
                'ProductDescription' : 1
            });

            route.addNode('ProductDescription', {
                'ProductModelProductDescription': 1
            });

            route.addNode('Address', {
                'CustomerAddress': 1,
                'SalesOrderHeader' : 1
            });

            route.addNode('CustomerAddress', {
                'Address': 1,
                'Customer' : 1
            });

            // figure out all the tables we need from our query
            var pathArrays = [],
                relationsRequired = [];

            pairs.forEach(pair => {
                var path = route.path(pair[0], pair[1]);
                console.log(`Pair: ${pair[0]} to ${pair[1]}: ${path}`);
                pathArrays = pathArrays.concat(path);
            });

            var tablesRequired = [...new Set(pathArrays)];

            var tablePairs = tablesRequired.reduce((acc, v, i) =>
                acc.concat(tablesRequired.slice(i + 1).map(w => [v, w])),
                []);

            // determine the relationships we need to JOIN the tables we found
            tablePairs.forEach(tablePair => {
                relationships.forEach(relation => {
                    if ((relation.pair[0] == tablePair[0] && relation.pair[1] == tablePair[1]) || (relation.pair[1] == tablePair[0] && relation.pair[0] == tablePair[1])) {
                        if (relationsRequired.indexOf(relation.relation) == -1) {
                            relationsRequired.push(relation.relation);
                        }
                    }
                });
            });

            // construct a SQL statement including the table names and the relationships we found
            var sqlParameter;
            if(sqlVal == 'count' || sqlVal == 'COUNT') { sqlParameter = '*'; }
            else { sqlParameter = columnVal; }

            var statement;
            //Case 1: Only querying a single table
            if(values.table_name != "" && values.column_name == "" && dataArr.length == 0) {
                statement = `SELECT\n\t${sqlVal}(*)\nFROM\n\t${'\tSalesLT.' + tableVal}`;
            }
            //Case 2: Only querying a single column from a single table
            else if(values.table_name == "" && values.column_name != "" && dataArr.length == 0) {
                statement = `SELECT\n\t${sqlVal}(${columnVal})\nFROM\n\t${'\tSalesLT.' + tableVal}`;
            }
            //Case 3 : Querying data from multiple tables
            else {
                statement = `SELECT\n\t${sqlVal}(${sqlParameter})\nFROM\n${tablesRequired.map(t => '\tSalesLT.' + t).join(',\n')}\nWHERE\n\t${relationsRequired.join(' AND\n\t')};`;
            }

            console.log("Statement is:\n", statement);

            // Connect to database, query with statement, send response to Alexa
            var returnText = "Something went wrong, sire.";
            sql.connect("mssql://winternsadmin:January999!@urtinterns.database.windows.net/adventureworks?encrypt=true")
                .then(pool => {
                    return pool.request()
                        .query(statement)
                        .then(result => {
                            if(result.recordset && result.recordset.toTable().rows.length>0) {
                                console.dir("The answer is " + result.recordset.toTable().rows[0].toString());
                                returnText = "The answer is " + result.recordset.toTable().rows[0].toString();
                            }
                            else {
                                returnText = "Zero rows were returned from the database sire.";
                            }
                            console.log(returnText);
                            return returnText;
                        })
                        .then(returnText => {
                            // Send response to Alexa Skill
                            self.response.speak(returnText);
                            self.emit(':responseReady');
                            console.log("Closing sql now...");
                            sql.close();
                            console.log("sql closed");
                        })
                        .catch((e) => {
                            console.error(e);
                            returnText = "That was not something I can query from the database. Please try again, sire.";
                            //returnText = "There was a problem with the SQL connection." + e.toString();
                            self.response.speak(returnText);
                            self.emit(':responseReady');
                            console.log("Closing sql now...");
                            sql.close();
                            console.log("sql closed");
                        })
                        .finally(() => {
                            // console.log("Closing SQL connection");
                            // sql.close(); 
                            console.log("Done");
                        });

                })
                .catch(() => {
                    self.response.speak("Cannot connect to db");
                }); 

         });

        request.on('error', function(error) {
            this.response.speak("The query was boned, sire.");
            this.emit(':responseReady');
            console.log(error);
        });

        request.end();
    },
    // Default Alexa help intent
    'AMAZON.HelpIntent': function () {
        const speechOutput = HELP_MESSAGE;
        const reprompt = HELP_REPROMPT;

        this.response.speak(speechOutput).listen(reprompt);
        this.emit(':responseReady');
    },
    // Default Alexa cancel intent
    'AMAZON.CancelIntent': function () {
        this.response.speak(STOP_MESSAGE);
        this.emit(':responseReady');
    },
    // Default Alexa stop intent
    'AMAZON.StopIntent': function () {
        this.response.speak(STOP_MESSAGE);
        this.emit(':responseReady');
    },
};
