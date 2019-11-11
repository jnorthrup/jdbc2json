package com.fnreport

import com.fnreport.mapper.ByteDataFrame
import com.fnreport.mapper.Codex

class SubFrame(newCodex: Codex, parent: ByteDataFrame) :
    ByteDataFrame(newCodex, parent.buffer.duplicate(), parent.recordLen, parent.size)