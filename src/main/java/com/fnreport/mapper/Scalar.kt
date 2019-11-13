package com.fnreport.mapper

interface Scalar<T> {
operator fun   get(row:Int):T
}
