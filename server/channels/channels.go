package channels

import "github.com/yousuf64/compute-uid/server/messages"

var UpdateCounter = make(chan *messages.UpdateCounterMessage)
var UpdateETag = make(chan *messages.UpdateETagMessage)
var FlushQueue = make(chan *messages.FlushQueueMessage)
var InvalidateBucket = make(chan *messages.InvalidateBucketMessage)
