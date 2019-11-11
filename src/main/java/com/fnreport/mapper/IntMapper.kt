package com.fnreport.mapper

val IntMapper: FieldParser<Int?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()?.toInt()
}