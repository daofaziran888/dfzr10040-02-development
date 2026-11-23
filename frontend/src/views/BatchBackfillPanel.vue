<template>
  <section class="batch-panel" data-testid="batch-backfill-panel">
    <header class="batch-head">
      <div>
        <h3>按日期批量回填</h3>
        <p class="page-desc">
          同一宽表的多个业务日一次提交，逐日补写并留下分区回执；下游只读取已完整提交的分区。
        </p>
      </div>
      <button class="btn" data-testid="batch-refresh" type="button" @click="refreshAll">刷新回执</button>
    </header>

    <div class="batch-form">
      <label>
        <span>目标宽表</span>
        <input v-model="wideTable" data-testid="batch-wide-table" />
      </label>
      <label v-for="(date, index) in dates" :key="index">
        <span>业务日 {{ index + 1 }}</span>
        <input v-model="dates[index]" :data-testid="`batch-date-${index}`" />
      </label>
      <button class="btn ghost" type="button" data-testid="batch-add-date" @click="dates.push('')">加一个日期</button>
    </div>

    <div class="batch-rows">
      <article v-for="date in dates" :key="`rows-${date}`" class="batch-row-card">
        <h4>{{ date || '未命名日期' }}</h4>
        <div v-for="(row, index) in rowsFor(date)" :key="index" class="batch-row-line">
          <input v-model="row.order_id" :data-testid="`row-order-${date}-${index}`" placeholder="订单号" />
          <input v-model.number="row.amount" :data-testid="`row-amount-${date}-${index}`" placeholder="金额" type="number" />
          <button class="link" type="button" @click="removeRow(date, index)">移除</button>
        </div>
        <button class="link" type="button" :data-testid="`row-add-${date}`" @click="addRow(date)">补一行</button>
      </article>
    </div>

    <div class="page-actions batch-actions">
      <button class="btn primary" data-testid="batch-submit" type="button" :disabled="busy" @click="submit">
        提交批量回填
      </button>
      <button class="btn" data-testid="batch-verify" type="button" :disabled="busy" @click="verify">
        核验缺回执日期
      </button>
      <button class="btn" data-testid="batch-export" type="button" :disabled="busy" @click="exportData">
        从快照导出取数
      </button>
      <button class="btn" data-testid="batch-confirm" type="button" :disabled="busy" @click="confirm">
        交分区快照确认
      </button>
    </div>

    <p v-if="message" class="batch-message" data-testid="batch-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="batch-error">{{ errorMessage }}</p>

    <h4>回填分区回执</h4>
    <table class="data-table" data-testid="receipt-table">
      <thead>
        <tr>
          <th>分区键值</th>
          <th>补写行数</th>
          <th>已提交行</th>
          <th>待核行</th>
          <th>实际写入行</th>
          <th>回执状态</th>
          <th>实际生效</th>
          <th>重试次数</th>
          <th>说明</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="receipt in receipts" :key="receipt.回执编号" :data-testid="`receipt-${receipt.分区键值}`" :data-status="receipt.回执状态">
          <td>{{ receipt.分区键值 }}</td>
          <td>{{ receipt.补写行数 }}</td>
          <td>{{ receipt.已提交行 }}</td>
          <td>{{ receipt.待核行 }}</td>
          <td>{{ receipt.实际写入行 }}</td>
          <td>{{ receipt.回执状态 }}</td>
          <td>{{ receipt.实际生效 }}</td>
          <td>{{ receipt.重试次数 }}</td>
          <td>{{ receipt.说明 }}</td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="9" class="empty-state">暂无回填分区回执</td>
        </tr>
      </tbody>
    </table>

    <h4>仍待核的业务日（交给值班续办）</h4>
    <table class="data-table" data-testid="pending-table">
      <thead>
        <tr><th>分区键值</th><th>已提交行</th><th>待核行</th><th>实际写入行</th><th>回执编号</th><th>操作</th></tr>
      </thead>
      <tbody>
        <tr v-for="item in pending" :key="item.分区键值" :data-testid="`pending-${item.分区键值}`">
          <td>{{ item.分区键值 }}</td>
          <td>{{ item.已提交行 }}</td>
          <td>{{ item.待核行 }}</td>
          <td>{{ item.实际写入行 }}</td>
          <td>{{ item.回执编号 }}</td>
          <td>
            <button
              class="link"
              type="button"
              :data-testid="`retry-${item.分区键值}`"
              @click="retry(item.分区键值)"
            >重试该分区</button>
          </td>
        </tr>
        <tr v-if="!pending.length">
          <td colspan="6" class="empty-state">没有待核业务日</td>
        </tr>
      </tbody>
    </table>

    <template v-if="exportResult">
      <h4>导出的订单行</h4>
      <p class="page-desc">
        可读分区：{{ exportResult.readable_dates.join('、') || '无' }}；
        跳过分区：{{ skippedText }}；共 {{ exportResult.exported_rows }} 行。
      </p>
      <table class="data-table" data-testid="export-table">
        <thead>
          <tr><th>日期</th><th>订单号</th><th>金额</th><th>入账凭据</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in exportResult.rows" :key="index" :data-testid="`export-row-${row.日期}-${row.订单号}`">
            <td>{{ row.日期 }}</td>
            <td>{{ row.订单号 }}</td>
            <td>{{ row.金额 }}</td>
            <td>{{ row.入账凭据 }}</td>
          </tr>
          <tr v-if="!exportResult.rows.length">
            <td colspan="4" class="empty-state">没有可读分区，导出为空</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  confirmSnapshot,
  exportReadable,
  listBackfillReceipts,
  listPendingPartitions,
  retryPartition,
  submitBatchBackfill,
  verifyReceipts,
  type BackfillReceipt,
  type BatchRow,
  type ExportResult,
  type PendingPartition,
} from '@/api/backfill-batch'

const wideTable = ref('订单宽表_值班回填')
const dates = ref(['20261116', '20261117', '20261118'])
const rowsByDate = ref<Record<string, BatchRow[]>>({
  20261116: [{ order_id: 'K', amount: 11 }, { order_id: 'A-1016', amount: 101 }],
  20261117: [{ order_id: 'K', amount: 22 }, { order_id: 'A-1017', amount: 102 }],
  20261118: [{ order_id: 'K', amount: 33 }, { order_id: 'A-1018', amount: 103 }],
})
const receipts = ref<BackfillReceipt[]>([])
const pending = ref<PendingPartition[]>([])
const exportResult = ref<ExportResult | null>(null)
const taskCode = ref('')
const message = ref('')
const errorMessage = ref('')
const busy = ref(false)

const skippedText = computed(() =>
  (exportResult.value?.skipped_dates ?? [])
    .map((item) => `${item.分区键值}（待核 ${item.待核行} 行）`)
    .join('、') || '无',
)

function rowsFor(date: string): BatchRow[] {
  if (!rowsByDate.value[date]) rowsByDate.value[date] = []
  return rowsByDate.value[date]
}

function addRow(date: string) {
  rowsFor(date).push({ order_id: '', amount: 0 })
}

function removeRow(date: string, index: number) {
  rowsFor(date).splice(index, 1)
}

async function refreshAll() {
  errorMessage.value = ''
  try {
    receipts.value = await listBackfillReceipts(taskCode.value || undefined)
    pending.value = await listPendingPartitions(taskCode.value || undefined)
    if (receipts.value.length) {
      taskCode.value = receipts.value[0].任务编号
      wideTable.value = receipts.value[0].目标宽表
      dates.value = receipts.value.map((item) => item.分区键值)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执刷新失败'
  }
}

async function submit() {
  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    const usableDates = dates.value.filter(Boolean)
    const result = await submitBatchBackfill(wideTable.value, usableDates, rowsByDate.value)
    message.value = result.message
    taskCode.value = result.task_code ?? taskCode.value
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量回填提交失败'
  } finally {
    busy.value = false
  }
}

async function retry(date: string) {
  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await retryPartition(taskCode.value, date)
    message.value = `${date} 重试完成：新增 ${result.rows_added} 行，跳过重复 ${result.duplicates_skipped} 行`
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区重试失败'
  } finally {
    busy.value = false
  }
}

async function verify() {
  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    await verifyReceipts(taskCode.value, wideTable.value, dates.value.filter(Boolean))
    message.value = '已核验缺回执或待核日期的实际写入'
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执核验失败'
  } finally {
    busy.value = false
  }
}

async function confirm() {  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await confirmSnapshot(wideTable.value, dates.value.filter(Boolean), taskCode.value)
    message.value = `分区快照已确认：生效 ${result.effective_dates.join('、') || '无'}；冲突 ${result.conflict_dates.join('、') || '无'}`
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照确认失败'
  } finally {
    busy.value = false
  }
}

async function exportData() {
  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    exportResult.value = await exportReadable(
      wideTable.value, dates.value.filter(Boolean), taskCode.value,
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照取数失败'
  } finally {
    busy.value = false
  }
}

onMounted(refreshAll)
</script>

<style scoped>
.batch-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin-bottom: 14px; }
.batch-head { display: flex; justify-content: space-between; align-items: flex-start; }
.batch-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; margin: 10px 0; }
.batch-form label span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.batch-form input { border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; }
.batch-rows { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 10px; }
.batch-row-card { flex: 1; min-width: 220px; border: 1px dashed var(--border); border-radius: 8px; padding: 10px; }
.batch-row-card h4 { margin: 0 0 6px; font-size: 13px; }
.batch-row-line { display: flex; gap: 6px; margin-bottom: 6px; }
.batch-row-line input { width: 100%; border: 1px solid var(--border); border-radius: 6px; padding: 4px 6px; font-size: 13px; }
.batch-actions { margin-bottom: 8px; justify-content: flex-start; }
.batch-message { color: #1f6feb; font-size: 13px; }
</style>
