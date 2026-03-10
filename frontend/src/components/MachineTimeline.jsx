import React, { useMemo } from 'react';

const BAR_COLOR = '#7a8fa9';

const formatClock = (value) => new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

const toDate = (value) => {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
};

const MachineTimeline = ({ rows = [], className = '' }) => {
  const timelineData = useMemo(() => {
    const validRows = rows
      .map((row) => {
        const start = toDate(row.start_time);
        const end = toDate(row.end_time);
        const machine = row.machine_name || row.assigned_machine || '-';
        if (!start || !end || machine === '-') return null;
        return {
          machine,
          jobLabel: row.job_id || row.job_name || '-',
          workerLabel: row.worker_name || row.assigned_worker || '-',
          startMs: start.getTime(),
          endMs: end.getTime(),
        };
      })
      .filter(Boolean);

    if (!validRows.length) {
      return { dayStart: 0, span: 24 * 60 * 60 * 1000, data: [], tickHours: [] };
    }

    const minStart = Math.min(...validRows.map((r) => r.startMs));
    const base = new Date(minStart);
    const dayStart = new Date(base.getFullYear(), base.getMonth(), base.getDate(), 0, 0, 0, 0).getTime();
    const span = 24 * 60 * 60 * 1000;

    const data = validRows.map((r, idx) => ({
      id: `${r.machine}-${r.jobLabel}-${idx}`,
      machine: r.machine,
      jobLabel: r.jobLabel,
      workerLabel: r.workerLabel,
      offset: r.startMs - dayStart,
      duration: Math.max(5 * 60 * 1000, r.endMs - r.startMs),
      startMs: r.startMs,
      endMs: r.endMs,
    }));
    const tickHours = Array.from({ length: 25 }, (_, h) => h);
    return { dayStart, span, data, tickHours };
  }, [rows]);

  if (!timelineData.data.length) {
    return <div className="timeline-empty">No scheduled jobs to visualize yet.</div>;
  }

  return (
    <div className={`timeline-wrap ${className}`}>
      <div className="timeline-hour-header">
        {timelineData.tickHours.map((hour) => (
          <div className="timeline-hour-tick" key={hour}>
            {hour % 2 === 0 ? `${String(hour).padStart(2, '0')}:00` : ''}
          </div>
        ))}
      </div>
      {timelineData.data.map((item) => {
        const leftMs = Math.max(0, item.offset);
        const rightMs = Math.min(timelineData.span, item.offset + item.duration);
        const clippedDuration = Math.max(0, rightMs - leftMs);
        const left = (leftMs / timelineData.span) * 100;
        const width = (clippedDuration / timelineData.span) * 100;
        return (
          <div className="timeline-box-row" key={item.id}>
            <div className="timeline-box-meta">
              <div className="timeline-box-machine">{item.machine}</div>
              <div className="timeline-box-job">{item.jobLabel} | {item.workerLabel}</div>
            </div>
            <div className="timeline-box-track">
              {Array.from({ length: 24 }, (_, i) => (
                <span
                  key={`${item.id}-grid-${i}`}
                  className="timeline-hour-line"
                  style={{ left: `${(i / 24) * 100}%` }}
                />
              ))}
              <div
                className="timeline-box-bar"
                style={{
                  left: `${Math.max(0, left)}%`,
                  width: `${Math.max(8, width)}%`,
                  background: BAR_COLOR,
                }}
                title={`${formatClock(item.startMs)} - ${formatClock(item.endMs)}`}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
};

export default MachineTimeline;
