/** 按日期批量回填接口：批量补写、分区回执、核验、重试与快照取数。 */
import { request } from './client'

export type BackfillReceipt = {
  回执编号: string
  任务编号: string
  目标宽表: string
  分区键值: string
  补写行数: string
  已提交行: string
  待核行: string
  实际写入行: string
  回执状态: string
  实际生效: string
  回执已交付: string
  重试次数: string
  回执时间: string
  说明: string
}

export type PendingPartition = {
  任务编号: string
  目标宽表: string
  分区键值: string
  已提交行: number
  待核行: number
  实际写入行: number
  回执编号: string
  说明: string
}

export type ExportRow = {
  日期: string
  订单号: string
  金额: number
  入账凭据: string
}

export type ExportResult = {
  ok: boolean
  plan_code: string
  target_wide_table: string
  requested_dates: string[]
  readable_dates: string[]
  effective_dates: string[]
  skipped_dates: { 分区键值: string; 待核行: number; 原因: string }[]
  rows: ExportRow[]
  exported_rows: number
  by_date: Record<string, number>
}

export type BatchRow = { order_id: string; amount: number }

async function postJson<T>(path: string, payload: unknown): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(payload) })
  if (!response.ok) throw new Error(`接口返回 ${response.status}，批量回填未更新`)
  return (await response.json()) as T
}

export async function listBackfillReceipts(taskCode?: string): Promise<BackfillReceipt[]> {
  const query = taskCode ? `?task_code=${encodeURIComponent(taskCode)}` : ''
  const response = await request(`/api/backfill-batch/receipts${query}`)
  if (!response.ok) throw new Error('回填分区回执读取失败')
  const payload = (await response.json()) as { items: BackfillReceipt[] }
  return payload.items
}

export async function listPendingPartitions(taskCode?: string): Promise<PendingPartition[]> {
  const query = taskCode ? `?task_code=${encodeURIComponent(taskCode)}` : ''
  const response = await request(`/api/backfill-batch/pending${query}`)
  if (!response.ok) throw new Error('待核日期读取失败')
  const payload = (await response.json()) as { dates: PendingPartition[] }
  return payload.dates
}

export async function createBatchTask(
  targetWideTable: string,
  dates: string[],
): Promise<{ task_code: string }> {
  return postJson('/api/backfill-batch/tasks', { target_wide_table: targetWideTable, dates })
}

export async function submitBatchBackfill(
  targetWideTable: string,
  dates: string[],
  rowsByDate: Record<string, BatchRow[]>,
  deliverReceiptDates?: string[],
): Promise<{ ok: boolean; message: string; task_code: string; receipts?: BackfillReceipt[] }> {
  return postJson('/api/backfill-batch/submit', {
    target_wide_table: targetWideTable,
    dates,
    rows_by_date: rowsByDate,
    deliver_receipt_dates: deliverReceiptDates,
  })
}

export async function verifyReceipts(
  taskCode?: string,
  targetWideTable = '',
  dates: string[] = [],
): Promise<void> {
  await postJson('/api/backfill-batch/verify', {
    task_code: taskCode,
    target_wide_table: targetWideTable,
    dates,
  })
}

export async function retryPartition(
  taskCode: string,
  partitionKeyValue: string,
): Promise<{ ok: boolean; message: string; rows_added: number; duplicates_skipped: number; committed_rows: number; pending_rows: number }> {
  return postJson('/api/backfill-batch/retry', {
    task_code: taskCode,
    partition_key_value: partitionKeyValue,
  })
}

export async function confirmSnapshot(
  targetWideTable: string,
  dates: string[],
  taskCode?: string,
): Promise<{ effective_dates: string[]; conflict_dates: string[]; pending_dates: { 分区键值: string; 待核行: number }[] }> {
  return postJson('/api/backfill-batch/confirm', {
    target_wide_table: targetWideTable,
    dates,
    snapshot_statuses: Object.fromEntries(dates.map((date) => [date, '已完成'])),
    task_code: taskCode,
  })
}

export async function exportReadable(
  targetWideTable: string,
  dates: string[],
  taskCode?: string,
): Promise<ExportResult> {
  return postJson('/api/backfill-batch/export', {
    target_wide_table: targetWideTable,
    dates,
    task_code: taskCode,
  })
}
