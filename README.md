# Object Holder

Small script to set/renew the [object lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html) on some or all of the objects in a given s3[-compatible] bucket.

Some modern backup software (eg, restic) doesn't do a periodic full backup like older software does, which is good for efficiency, but this means that a given restore might depend on any file in the backup repo, even possibly one from the first backup. Because of this, just setting a default object lock duration on the bucket you're backing up to isn't enough to cover everything, since the object you need from the bucket might outlive that. Ideally, the backup software would be object-lock-aware, so it can manage the objects locks itself, but when that's not the case, this script might come in handy.

## Usage

```
object-holder
  [--endpoint <s3 endpoint>]
  [--region <s3 region>]
  [--access-key-id <aws access key id>]
  [--secret-access-key <aws access key secret>]
  --bucket <bucket name>
  [--prefix <prefix>]
  [--update-expires-within <seconds>]
  --lock-for <seconds>
  [--threads <thread count>]
```

If you're using name-brand S3, you don't need to specify endpoint, just the region, otherwise, you'll need to specify the endpoint and possibly also the region.

The aws keys can be fed in via the cli flags, via the usual env variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`), via instance metadata, etc.

`--bucket`, obviously, specifies the bucket you'd like to renew object locks in, and `--prefix` can be used to limit operation to only the objects with a name starting with a given prefix in the bucket.

`--lock-for` specifies how many seconds the newly-made locks should last, counting from time of program start.

`--update-expires-within-seconds` can be used to limit operation to only objects whose existing lock expires within that many seconds. This might save you a bit depending on your s3 provider's API request pricing. For example, in name-brand S3, it costs about 10x more to set a lock than it does to check it, so this flag might save you a bit there. Meanwhile, in Backblaze B2, setting an object hold is free but checking an existing hold costs money, so there's no reason to do the extra check.

Lastly, `--threads` specifies how many concurrent threads we should have getting/setting object holds (default 1024). Some s3-compatible providers' APIs seem to misbehave when sequentially updating object metadata at high speeds like this script does, so here's a handy knob to adjust the speed in case you need it.
