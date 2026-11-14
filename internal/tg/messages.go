package tg

import "github.com/gotd/td/tg"

// MessagesFrom extracts the message list from any messages.* response.
func MessagesFrom(res tg.MessagesMessagesClass) []tg.MessageClass {
	switch v := res.(type) {
	case *tg.MessagesMessages:
		return v.Messages
	case *tg.MessagesMessagesSlice:
		return v.Messages
	case *tg.MessagesChannelMessages:
		return v.Messages
	default:
		return nil
	}
}
