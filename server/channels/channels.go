package channels

import "unique-id-generator/server/messages"

var UpdateCounter = make(chan *messages.UpdateCounterMessage)
var UpdateETag = make(chan *messages.UpdateETagMessage)
var FlushQueue = make(chan *messages.FlushQueueMessage)
var InvalidateBucket = make(chan *messages.InvalidateBucketMessage)
