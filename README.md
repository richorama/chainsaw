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

# License

MIT
