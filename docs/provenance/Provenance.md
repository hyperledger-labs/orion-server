## Provenance
Provenance API give access to following data:
- `key->value` history, in different views and directions
- info about users who accessed specific piece of data
- info, including history, about data accessed by user
- history of user transactions

Provenance data exposed by server as REST API. It can be accessed directly, see [here](../curl/provenance.md) or through multiple sdks, for example, GO see [here](https://github.com/IBM-Blockchain/bcdb-sdk/tree/main/docs/provenance.md)

### Provenance data

__Key->Value history data__

Most of historical data queries return multiple records that describe changes of the value associated with specific key over time. 
> Time in BCDB is discrete and associated with block number, so we can say "value of key X in block N was Y". 

```protobuf
message GetHistoricalDataResponse {
  ResponseHeader header = 1;
  repeated ValueWithMetadata values = 2;
}

message ValueWithMetadata{
  bytes value = 1;
  Metadata metadata = 2;
}

message Metadata {
  Version version = 1;
  AccessControl access_control = 2;
}

message Version {
  uint64 block_num = 1;
  uint64 tx_num = 2;
}
```

__Users who accessed specific piece of data__

This type of queries return list of users and number of times this data was accessed by them.

```protobuf
message GetDataReadersResponse {
  ResponseHeader header = 1;
  map<string, uint32> read_by = 2;
}

message GetDataWritersResponse {
  ResponseHeader header = 1;
  map<string, uint32> written_by = 2;
}
```

__All user transactions__

Just list of TxIDs of transactions submitted by user and recorded in ledger, no matter if these transactions are valid or not.

```protobuf
message GetTxIDsSubmittedByResponse {
  ResponseHeader header = 1;
  repeated string txIDs = 2;
}
```

__Data accessed by user__

This query returns all keys accessed by specific user, including time (version) they were accessed.
For example, if transaction 1 in block 5 read `key1`, one of values in list will contain `{key: "key1", value: "...", Version : {block_num: 5, tx_num: 1}, ...}` 

```protobuf
message GetDataProvenanceResponse {
  ResponseHeader header = 1;
  repeated KVWithMetadata KVs = 2;
}

message KVWithMetadata{
  string key = 1;
  bytes value = 2;
  Metadata metadata = 3;
}

message Metadata {
  Version version = 1;
  AccessControl access_control = 2;
}

message Version {
  uint64 block_num = 1;
  uint64 tx_num = 2;
}
```