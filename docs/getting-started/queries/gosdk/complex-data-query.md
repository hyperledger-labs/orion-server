---
id: complex-data-query
title: Query Data Using Fields in a JSON object
---

Once a database session is created, we can call `session.JSONQuery()` to get a `JSONQuery` object.

```go
// JSONQuery provides method to execute json query on a given user database
// The query is a json string which must contain predicates under the field
// selector. The first field in the selector can be a combinational operator
// such as "$and" or "$or" followed by a list of attributes and a list of
// conditions per attributes. A query example is shown below
//
// {
//   "selector": {
// 		"$and": {            -- top level combinational operator
// 			"attr1": {          -- a field in the json document
// 				"$gte": "a",    -- value criteria for the field
// 				"$lt": "b"      -- value criteria for the field
// 			},
// 			"attr2": {          -- a field in the json document
// 				"$eq": true     -- value criteria for the field
// 			},
// 			"attr3": {          -- a field in the json document
// 				"$lt": "a2"     -- a field in the json document
// 			}
// 		}
//   }
// }
type JSONQuery interface {
	Execute(dbName, query string) ([]*types.KVWithMetadata, error)
}
```

To query data, we need to call `Execute()` method of the object from above.

<details>
<summary> Pre-requisite: Create db3 with index, provide read-write permission on db3 to Alice, write sample data for queries</summary>

```go
package main

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func main() {
	db, err := createConnection()
    // if err is not nil, print and return

	session, err := openSession(db, "admin")
    // if err is not nil, print and return

	err = createDBWithIndex(session)
    // if err is not nil, print and return

	err = createAlice(session)
    // if err is not nil, print and return

	session, err = openSession(db, "alice")
    // if err is not nil, print and return

	tx, err := session.DataTx()
    // if err is not nil, print and return

	tx.Put("db3", "person1", []byte(`{"name":"a","age":18,"graduated":false,"rating":-234`), nil)
	tx.Put("db3", "person2", []byte(`{"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}`), nil)
	tx.Put("db3", "person3", []byte(`{"name":"c","age":20,"graduated":false,"rating":-4}`), nil)
	tx.Put("db3", "person4", []byte(`{"name":"d","age":19,"graduated":false,"rating":-1}`), nil)
	tx.Put("db3", "person5", []byte(`{"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}`), nil)
	tx.Put("db3", "person6", []byte(`{"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}`), nil)
	tx.Put("db3", "person7", []byte(`{"name":"g","age":28,"graduated":false,"rating":100}`), nil)
	tx.Put("db3", "person8", []byte(`{"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}`), nil)

	txID, receipt, err := tx.Commit(true)
    // if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
}

func createDBWithIndex(s bcdb.DBSession) error {
	tx, err := s.DBsTx()
	// if err is not nil, print and return

	index := map[string]types.IndexAttributeType{
		"name":      types.IndexAttributeType_STRING,
		"age":       types.IndexAttributeType_NUMBER,
		"graduated": types.IndexAttributeType_BOOLEAN,
		"rating":    types.IndexAttributeType_NUMBER,
	}

	err = tx.CreateDB("db3", index)
	// if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
	// if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
	return nil
}

func createAlice(s bcdb.DBSession) error {
	alicePemUserCert, err := ioutil.ReadFile("./crypto/alice/alice.pem")
	// if err is not nil, print and return

	aliceCertBlock, _ := pem.Decode(alicePemUserCert)

	alice := &types.User{
		Id:          "alice",
		Certificate: aliceCertBlock.Bytes,
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db3": types.Privilege_ReadWrite,
			},
		},
	}

	tx, err := s.UsersTx()
	// if err is not nil, print and return

	err = tx.PutUser(alice, nil)
	// if err is not nil, print and return

	txID, receipt, err := tx.Commit(true)
	// if err is not nil, print and return

	fmt.Println("transaction with txID " + txID + " got committed in the block " + strconv.Itoa(int(receipt.GetHeader().GetBaseHeader().GetNumber())))
	return nil
}
```

</details>

## 1) Equal Operator (`$eq`)
```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	// if err is not nil, print and return

	session, err := openSession(db, "alice")
	// if err is not nil, print and return

	q, err := session.JSONQuery()
	// if err is not nil, print and return

	queryStr := `
	{
		"selector": {
			"graduated": {
				"$eq":false
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"name": {
				"$eq":"c"
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$eq":-1
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
============================
results for
        {
                "selector": {
                        "graduated": {
                                "$eq":false
                        }
                }

        }

person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
============================
results for
        {
                "selector": {
                        "name": {
                                "$eq":"c"
                        }
                }

        }

person3: {"name":"c","age":20,"graduated":false,"rating":-4}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$eq":-1
                        }
                }

        }

person4: {"name":"d","age":19,"graduated":false,"rating":-1}
```

</details>

## 2) Not Equal Operator (`$neq`)

```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	// if err is not nil, print and return

	session, err := openSession(db, "alice")
	// if err is not nil, print and return

	q, err := session.JSONQuery()
	// if err is not nil, print and return

	queryStr := `
	{
		"selector": {
			"graduated": {
				"$neq": [false]
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"name": {
				"$neq":["a", "b", "c", "d"]
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$neq":[-1, 100, -100]
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
============================
results for
        {
                "selector": {
                        "graduated": {
                                "$neq": [false]
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
============================
results for
        {
                "selector": {
                        "name": {
                                "$neq":["a", "b", "c", "d"]
                        }
                }

        }

person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$neq":[-1, 100, -100]
                        }
                }

        }

person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
```

</details>

## 3) Greater than (`$gt`) Operator
```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	// if err is not nil, print and return

	session, err := openSession(db, "alice")
	// if err is not nil, print and return

	q, err := session.JSONQuery()
	// if err is not nil, print and return

	queryStr := `
	{
		"selector": {
			"graduated": {
				"$gt": false
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"name": {
				"$gt":"e"
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$gt":-4
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
============================
results for
        {
                "selector": {
                        "graduated": {
                                "$gt": false
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
============================
results for
        {
                "selector": {
                        "name": {
                                "$gt":"e"
                        }
                }

        }

person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$gt":-4
                        }
                }

        }

person7: {"name":"g","age":28,"graduated":false,"rating":100}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
```

</details>

## 4) Lesser than (`$lt`) Operator

```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	// if err is not nil, print and return

	session, err := openSession(db, "alice")
	// if err is not nil, print and return

	q, err := session.JSONQuery()
	// if err is not nil, print and return

	queryStr := `
	{
		"selector": {
			"graduated": {
				"$lt": true
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"name": {
				"$lt":"e"
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$lt":-4
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
============================
results for
        {
                "selector": {
                        "graduated": {
                                "$lt": true
                        }
                }

        }

person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
============================
results for
        {
                "selector": {
                        "name": {
                                "$lt":"e"
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$lt":-4
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
```

</details>

## 5) Greater than or equal (`$gte`) Operator

```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	// if err is not nil, print and return

	session, err := openSession(db, "alice")
	// if err is not nil, print and return

	q, err := session.JSONQuery()
	// if err is not nil, print and return

	queryStr := `
	{
		"selector": {
			"graduated": {
				"$gte": false
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"name": {
				"$gte":"e"
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$gte":-4
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
results for
        {
                "selector": {
                        "graduated": {
                                "$gte": false
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
============================
results for
        {
                "selector": {
                        "name": {
                                "$gte":"e"
                        }
                }

        }

person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$gte":-4
                        }
                }

        }

person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
```

</details>

## 6) Lesser than or equal (`$gte`) Operator

```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	// if err is not nil, print and return

	session, err := openSession(db, "alice")
	// if err is not nil, print and return

	q, err := session.JSONQuery()
	// if err is not nil, print and return

	queryStr := `
	{
		"selector": {
			"graduated": {
				"$lte": true
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"name": {
				"$lte":"e"
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$lte":-4
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
============================
results for
        {
                "selector": {
                        "graduated": {
                                "$lte": true
                        }
                }

        }

person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
============================
results for
        {
                "selector": {
                        "name": {
                                "$lte":"e"
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$lte":-4
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
```

</details>

## 7) Multiple Logical Operators (`$gt`, `$lt`, `$gte`, `$lte`,`$neq`)

```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	// if err is not nil, print and return

	session, err := openSession(db, "alice")
	// if err is not nil, print and return

	q, err := session.JSONQuery()
	// if err is not nil, print and return

	queryStr := `
	{
		"selector": {
			"name": {
				"$gt": "e",
				"$lte":"h"
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$gt":-100,
				"$lt":100
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return

	queryStr = `
	{
		"selector": {
			"rating": {
				"$gte":-100,
				"$lte":100,
				"$neq": [-1, 0, 12]
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	// if err is not nil, print and return
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
============================
results for
        {
                "selector": {
                        "name": {
                                "$gt": "e",
                                "$lte":"h"
                        }
                }

        }

person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$gt":-100,
                                "$lt":100
                        }
                }

        }

person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
============================
results for
        {
                "selector": {
                        "rating": {
                                "$gte":-100,
                                "$lte":100,
                                "$neq": [-1, 0, 12]
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
```

</details>

## 8) Combinational Operators - `$and` and `$or`

```go
package main

import (
	"fmt"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
)

func main() {
	db, err := createConnection()
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	session, err := openSession(db, "alice")
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}

	q, err := session.JSONQuery()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	queryStr := `
	{
		"selector": {
			"$and": {
				"name": {
					"$gt": "e",
					"$lte":"h"
				},
				"graduated": {
					"$eq": false
				},
				"rating": {
					"$gt": -100,
					"$neq": [0]
				}
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	queryStr = `
	{
		"selector": {
			"$and": {
				"name": {
					"$gte": "a",
					"$neq": ["d", "e"],
					"$lte":"h"
				},
				"graduated": {
					"$eq": true
				},
				"rating": {
					"$gte": -1000,
					"$neq": [0],
					"$lte": 1000
				}
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	queryStr = `
	{
		"selector": {
			"$or": {
				"name": {
					"$gt": "e",
					"$lte":"h"
				},
				"graduated": {
					"$eq": false
				},
				"rating": {
					"$gt": -100,
					"$neq": [0]
				}
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	queryStr = `
	{
		"selector": {
			"$or": {
				"name": {
					"$gte": "a",
					"$neq": ["d", "e"],
					"$lte":"h"
				},
				"graduated": {
					"$eq": true
				},
				"rating": {
					"$gte": -1000,
					"$neq": [0],
					"$lte": 1000
				}
			}
		}

	}
	`
	err = executeAndPrint(q, queryStr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func executeAndPrint(q bcdb.JSONQuery, queryStr string) error {
	kvs, err := q.Execute("db3", queryStr)
	if err != nil {
		return err
	}

	fmt.Println("============================")
	fmt.Println("results for " + queryStr)
	for _, kv := range kvs {
		fmt.Println(kv.Key + ": " + string(kv.Value))
	}

	return nil
}
```

<details>
<summary> Output </summary>

```webmanifest
============================
results for
        {
                "selector": {
                        "$and": {
                                "name": {
                                        "$gt": "e",
                                        "$lte":"h"
                                },
                                "graduated": {
                                        "$eq": false
                                },
                                "rating": {
                                        "$gt": -100,
                                        "$neq": [0]
                                }
                        }
                }

        }

person7: {"name":"g","age":28,"graduated":false,"rating":100}
============================
results for
        {
                "selector": {
                        "$and": {
                                "name": {
                                        "$gte": "a",
                                        "$neq": ["d", "e"],
                                        "$lte":"h"
                                },
                                "graduated": {
                                        "$eq": true
                                },
                                "rating": {
                                        "$gte": -1000,
                                        "$neq": [0],
                                        "$lte": 1000
                                }
                        }
                }

        }

person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
============================
results for
        {
                "selector": {
                        "$or": {
                                "name": {
                                        "$gt": "e",
                                        "$lte":"h"
                                },
                                "graduated": {
                                        "$eq": false
                                },
                                "rating": {
                                        "$gt": -100,
                                        "$neq": [0]
                                }
                        }
                }

        }

person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person7: {"name":"g","age":28,"graduated":false,"rating":100}
============================
results for
        {
                "selector": {
                        "$or": {
                                "name": {
                                        "$gte": "a",
                                        "$neq": ["d", "e"],
                                        "$lte":"h"
                                },
                                "graduated": {
                                        "$eq": true
                                },
                                "rating": {
                                        "$gte": -1000,
                                        "$neq": [0],
                                        "$lte": 1000
                                }
                        }
                }

        }

person7: {"name":"g","age":28,"graduated":false,"rating":100}
person2: {"name":"b","age":48,"graduated":true,"rating":-100,"degree":"bachelor"}
person3: {"name":"c","age":20,"graduated":false,"rating":-4}
person4: {"name":"d","age":19,"graduated":false,"rating":-1}
person6: {"name":"f","age":24,"graduated":true,"rating":12,"degree":"bachelor"}
person8: {"name":"h","age":38,"graduated":true,"rating":1230,"degree":"master"}
person5: {"name":"e","age":31,"graduated":true,"rating":0,"degree":"master"}
```

</details>
