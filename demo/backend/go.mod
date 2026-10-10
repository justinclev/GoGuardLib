module github.com/justinclev/GoGuardLib/demo/backend

go 1.25.3

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.11.1
	github.com/justinclev/GoGuardLib v0.0.0
	github.com/justinclev/GoGuardLib/kafka v0.0.0
)

replace github.com/justinclev/GoGuardLib => ../..

replace github.com/justinclev/GoGuardLib/kafka => ../../kafka
