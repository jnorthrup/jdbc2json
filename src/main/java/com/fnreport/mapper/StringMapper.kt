package com.fnreport.mapper

val StringMapper: FieldParser<String?> = {
    String(it).takeUnless(String::isBlank)?.trimEnd()
}