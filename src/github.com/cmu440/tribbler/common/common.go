package common

const (
	Layout = "01/02 03:04:05PM '06 -0700"
)

func toSubscribeList(userID string) string {
	return userID + ":subList"
}

func toTribbleList(userID string) string {
	return userID + ":tribbleList"
}
