package streams

import "unique-id-generator/server/messages"

var UpdateCounterMessage = make(chan messages.UpdateCounterMessage, 0)
var UpdateETagMessage = make(chan messages.UpdateETagMessage)
var FlushQueueMessage = make(chan *messages.FlushQueueMessage)
