[![Build status](https://ci.appveyor.com/api/projects/status/0re5ptmhiv3f4sy1?svg=true)](https://ci.appveyor.com/project/richorama/chainsaw)

# chainsaw
:evergreen_tree: :warning: highly experimental - avoid

# Usage

```c#
using (var db = new Database<MyPocoClass>("path/to/database"))
{
    // write a value
    db.Set("foo", new MyPocoClass());

    // read a value
    var value = db.Get("foo");

    // delete a value
    db.Delete("foo");
}
```
The db object should be created as a singleton in your application code, do not create/dispose is frequently.

The database is thread safe, and supports simultaneous read/write operations.

Batch writes are also supported:

```c#
using (var db = new Database<MyPocoClass>("path/to/database"))
{
    var batch = db.CreateBatch();

    batch.Set("one", new MyPocoClass());
    batch.Set("two", new MyPocoClass());
    batch.Set("three", new MyPocoClass());
    batch.Delete("zero");

    // write the batch to the database in one operation
    batch.Commit();
}

```

Scan through the data:

```c#
using (var db = new Database<MyPocoClass>("path/to/database"))
{
    foreach (var record in db.Scan())
    {
        record.Operation;   // 'Set' or 'Delete'
        record.Key;         
        record.Value;
        record.Tag;         // Tag, can be used as a cursor or version
    }
}
```
# License

MIT
