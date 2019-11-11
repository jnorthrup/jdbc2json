package com.fnreport

class SubFrame(newCodex: Codex, parent: ByteDataFrame) :
    ByteDataFrame(newCodex, parent.buffer.duplicate(), parent.recordLen, parent.size)