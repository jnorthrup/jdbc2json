package com.fnreport

open class EnvConfig(val name: String, val defValue: String? = null, val docString: String? = null) {
    private val env: String? by lazy { System.getenv(name) ?: defValue }
    val value get() = env
    override fun toString(): String = "$name${defValue?.let { ":='$defValue'" }
            ?: ""}${docString?.let { "/* $docString */" } ?: ""}"
}