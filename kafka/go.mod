module github.com/justinclev/GoGuardLib/kafka

go 1.25.3

toolchain go1.25.13

require github.com/justinclev/GoGuardLib v0.0.0

require github.com/confluentinc/confluent-kafka-go/v2 v2.11.1

// During development the core module is the one next door. Remove this line (and
// depend on a tagged release) when publishing.
replace github.com/justinclev/GoGuardLib => ../
