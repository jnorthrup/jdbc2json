package com.fnreport.mapper

val LongMapper: FieldParser<Long?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()?.toLong()
}