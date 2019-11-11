package com.fnreport.mapper

typealias codec = (ByteArray) -> Any
typealias IndirectName = () -> String
typealias ParseCoordinates = Pair<Int, Int>
typealias FieldParser<T> = (ByteArray) -> T?
typealias ParseDescriptor = Pair<IndirectName, ParseCoordinates>
typealias Decoder = Pair<ParseDescriptor, FieldParser<*>>
typealias Codex = Array<Decoder>
