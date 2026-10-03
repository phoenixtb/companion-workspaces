package com.shelfwise.part11

import com.shelfwise.part11.dashboard.ShelfScan
import com.shelfwise.part11.dashboard.StockBoard
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@OptIn(ExperimentalCoroutinesApi::class) // advanceTimeBy, runCurrent, currentTime
class StockBoardTest {
    @Test
    fun `a scan at or below the reorder point raises an alert`() = runTest {
        val board = StockBoard(reorderAt = 10)

        board.record(ShelfScan("PUN-014", "SHW-2040", 10))

        assertEquals(setOf("PUN-014/SHW-2040"), board.lowStock.first())
    }

    @Test
    fun `virtual time skips the waiting`() = runTest {
        val board = StockBoard()
        launch {
            delay(60_000) // a scanner that reports once a minute
            board.record(ShelfScan("MUM-002", "SHW-1001", 3))
        }

        advanceTimeBy(60_001) // instant in real time
        runCurrent()

        assertEquals(3, board.levels.value["MUM-002/SHW-1001"])
        assertEquals(60_001, testScheduler.currentTime)
    }
}
