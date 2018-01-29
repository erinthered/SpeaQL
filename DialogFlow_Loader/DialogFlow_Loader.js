'use strict';
var request = require('request');
var sql = require('mssql');

const tableSpecs = [
    {
        Customer: [
            "CompanyName"
        ]
    },
    {
        Product: [
            "Name",
            "Size",
            "Color"
        ]
    }
];

var headers = {
    'Authorization': ''
}

function getDatabaseValues(table, column, pool) {

    const statement = `SELECT DISTINCT(${column}) FROM SalesLT.${table} WHERE ${column} IS NOT NULL ORDER BY ${column}`; 

    return pool.request()
        .query(statement)
        .then(result => {
            return result.recordset.toTable().rows.map(row => {
                return row[0];
            });
        });
}

function processTableSpecs(pool) {

    tableSpecs.forEach(function(tableSpec) {

        var tableName = Object.keys(tableSpec)[0];
        
        var columns = tableSpec[tableName];

        columns.forEach(function(column) {
            var transformedTableName = tableName.replace( /[^A-Z]/g, '' ) + '-' + column + '-Data';

        // get the data from the table 
        getDatabaseValues(tableName, column, pool)
            .then(values => {

                var objectToSend = {
                    "name": transformedTableName,
                    "entries": values.map(value => {
                        return {
                            value: value,
                            synonyms: [value]
                        };
                    })
                };

                // send to dialogflow
                request.put({
                    url: "link",
                    headers: headers,
                    json: objectToSend
                },
                    function (err, response) {
                        if (err) {
                            console.error("Did not work! ", err);
                        }
                        else {
                            if(response.statusCode == 200) {
                                console.log("%s has been processed successfully with these values: %s", transformedTableName, values);
                            }
                            else {
                                console.error(response.body);
                            }
                        }
                    }
                )

            })
            

        });

    })
}

sql.connect("mssql:")
    .then(pool => {
        processTableSpecs(pool);
    });

