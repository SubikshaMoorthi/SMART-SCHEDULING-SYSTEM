import React, { useCallback, useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  Briefcase,
  Cpu,
  LayoutDashboard,
  LogOut,
  Play,
  UserCheck,
} from 'lucide-react';
import '../App.css';

const API = 'http://127.0.0.1:8000';

const navItems = [
  { key: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { key: 'jobs', label: 'My Jobs', icon: Briefcase },
  { key: 'attendance', label: 'Attendance', icon: UserCheck },
];

const apiClient = () => {
  const token = sessionStorage.getItem('token');
  return axios.create({
    baseURL: API,
    headers: { Authorization: `Bearer ${token}` },
  });
};

const formatAttendanceTime = (value, activeLabel = '-') => {
  if (!value) return activeLabel;
  const raw = String(value);
  const date = new Date(raw);
  if (!Number.isNaN(date.getTime())) return date.toLocaleString();
  if (raw.length >= 5) return raw.slice(0, 5);
  return raw;
};

const attendanceStatusText = (row) => row.status || row.attendance_status || '-';
const attendanceInTime = (row) => row.login_time || row.in_time;
const attendanceOutTime = (row) => row.logout_time || row.out_time;
const attendanceWhen = (row) => row.created_at || row.login_time;

const WorkerDashboard = () => {
  const [activeSection, setActiveSection] = useState('dashboard');
  const [profile, setProfile] = useState({});
  const [jobs, setJobs] = useState([]);
  const [attendance, setAttendance] = useState([]);
  const [attendanceStatus, setAttendanceStatus] = useState({ active_session: false, leave_status: 'On Leave' });
  const [message, setMessage] = useState('');
  const [jobActionChoice, setJobActionChoice] = useState({});
  const client = useMemo(() => apiClient(), []);

  const setNotice = (text) => {
    setMessage(text);
    window.setTimeout(() => setMessage(''), 3000);
  };

  const loadAll = useCallback(async () => {
    try {
      const [p, j, a] = await Promise.all([
        client.get('/worker/profile'),
        client.get('/worker/jobs'),
        client.get('/worker/attendance-history'),
      ]);
      const status = await client.get('/worker/attendance-status');
      setProfile(p.data);
      setJobs(j.data);
      setAttendance(a.data);
      setAttendanceStatus(status.data || { active_session: false, leave_status: 'On Leave' });
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Failed to load worker data');
    }
  }, [client]);

  useEffect(() => {
    const timer = window.setTimeout(() => loadAll(), 0);
    return () => window.clearTimeout(timer);
  }, [loadAll]);

  const startJob = async (source, jobId) => {
    try {
      await client.post('/worker/jobs/start', { source, job_id: jobId });
      setNotice('Job started');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to start job');
    }
  };

  const completeJob = async (source, jobId) => {
    try {
      await client.post('/worker/jobs/complete', { source, job_id: jobId });
      setNotice('Job marked completed');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to complete job');
    }
  };

  const reportDelay = async (source, jobId) => {
    try {
      await client.post('/worker/report-issue', {
        source,
        job_id: jobId,
        issue_type: 'job_delay',
        details: 'Worker reported delay',
      });
      setNotice('Delay reported');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to report delay');
    }
  };

  const reportBreakdown = async (source, jobId, machineId) => {
    try {
      await client.post('/worker/report-issue', {
        source,
        job_id: jobId,
        issue_type: 'machine_breakdown',
        machine_id: machineId,
        details: 'Worker reported machine breakdown',
      });
      setNotice('Machine breakdown reported');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to report machine issue');
    }
  };

  const checkIn = async () => {
    try {
      await client.post('/worker/attendance/in');
      setNotice('Attendance started');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to check in');
    }
  };

  const checkOut = async () => {
    try {
      await client.post('/worker/attendance/out');
      setNotice('Attendance closed');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to check out');
    }
  };

  const requestLeave = async () => {
    try {
      await client.post('/worker/attendance/leave', { reason: 'Requested by worker' });
      setNotice('Leave requested');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to request leave');
    }
  };

  const runJobAction = async (job) => {
    const key = `${job.source}-${job.job_id}-${job.start_time || 'x'}`;
    const action = jobActionChoice[key];
    if (!action) return;

    if (action === 'start') await startJob(job.source, job.job_id);
    if (action === 'complete') await completeJob(job.source, job.job_id);
    if (action === 'delay') await reportDelay(job.source, job.job_id);
    if (action === 'breakdown') await reportBreakdown(job.source, job.job_id, job.machine_id);

    setJobActionChoice((prev) => ({ ...prev, [key]: '' }));
  };

  const renderSection = () => {
    if (activeSection === 'dashboard') {
      const running = jobs.filter((j) => j.status === 'Running').length;
      const completed = jobs.filter((j) => j.status === 'Completed').length;
      return (
        <>
          <header className="hero">
            <div className="hero__text">
              <p className="hero__eyebrow">Worker Panel</p>
              <h1 className="hero__title">My Task Center</h1>
              <p className="hero__subtitle">Track assigned jobs, machine details, and attendance.</p>
            </div>
          </header>
          <div className="stats-grid">
            <StatCard label="Worker" value={profile.name || '-'} trend={profile.worker_id || '-'} icon={<UserCheck />} color="blue" />
            <StatCard label="Skills" value={profile.skills || '-'} trend={profile.availability || '-'} icon={<Cpu />} color="emerald" />
            <StatCard label="Running Jobs" value={running} trend="In Progress" icon={<Play />} color="amber" />
            <StatCard label="Completed Jobs" value={completed} trend="Output" icon={<Briefcase />} color="indigo" />
          </div>
          <section className="assignments-panel">
            <div className="assignments-panel__header"><h2>Worker Profile</h2></div>
            <div className="reports-kpi">
              <div className="kpi">Worker ID: {profile.worker_id || '-'}</div>
              <div className="kpi">Name: {profile.name || '-'}</div>
              <div className="kpi">Skill: {profile.skills || '-'}</div>
              <div className="kpi">Availability: {profile.availability || '-'}</div>
              <div className="kpi">Shift: {profile.shift || 'Day'}</div>
              <div className="kpi">In-Time: {profile.in_time || '--:--'}</div>
              <div className="kpi">Out-Time: {profile.out_time || '--:--'}</div>
            </div>
          </section>
        </>
      );
    }

    if (activeSection === 'jobs') {
      return (
        <section className="assignments-panel">
          <div className="assignments-panel__header"><h2>Assigned Jobs</h2></div>
          <div className="table-wrap">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Processing</th>
                  <th>Due Date</th>
                  <th>Priority</th>
                  <th>Machine</th>
                  <th>Start</th>
                  <th>End</th>
                  <th>Status</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {jobs.length > 0 ? (
                  jobs.map((job) => (
                    <tr key={`${job.source}-${job.job_id}-${job.start_time || 'x'}`}>
                      <td>{job.job_id} - {job.job_name}</td>
                      <td>{job.processing_time} min</td>
                      <td>{job.due_date ? new Date(job.due_date).toLocaleString() : '-'}</td>
                      <td>{job.priority}</td>
                      <td>{job.machine_name} ({job.machine_purpose})</td>
                      <td>{job.start_time ? new Date(job.start_time).toLocaleString() : '-'}</td>
                      <td>{job.end_time ? new Date(job.end_time).toLocaleString() : '-'}</td>
                      <td>{job.status}</td>
                      <td>
                        <div className="action-row">
                          <select
                            value={jobActionChoice[`${job.source}-${job.job_id}-${job.start_time || 'x'}`] || ''}
                            onChange={(e) =>
                              setJobActionChoice((prev) => ({
                                ...prev,
                                [`${job.source}-${job.job_id}-${job.start_time || 'x'}`]: e.target.value,
                              }))
                            }
                          >
                            <option value="">Choose action</option>
                            <option value="start">Start</option>
                            <option value="complete">Complete</option>
                            <option value="delay">Delay</option>
                            <option value="breakdown">Breakdown</option>
                          </select>
                          <button
                            className="mini-btn"
                            onClick={() => runJobAction(job)}
                            disabled={!jobActionChoice[`${job.source}-${job.job_id}-${job.start_time || 'x'}`]}
                          >
                            Apply
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan="9" className="empty-state-cell">No assigned jobs found.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      );
    }

    return (
      <section className="assignments-panel">
        <div className="assignments-panel__header"><h2>Attendance History</h2></div>
        <div className="action-row">
          {!attendanceStatus.active_session ? (
            <button className="generate-btn" onClick={checkIn}>In</button>
          ) : (
            <button className="generate-btn" onClick={checkOut}>Out</button>
          )}
          <button className="ghost-btn" onClick={requestLeave}>Leave Request</button>
          <div className="kpi">Status: {attendanceStatus.leave_status || '-'}</div>
          <div className="kpi">In-Time: {formatAttendanceTime(attendanceStatus.in_time, '--')}</div>
          <div className="kpi">Auto Out-Time: {formatAttendanceTime(attendanceStatus.expected_out_time, '--')}</div>
        </div>
        <div className="table-wrap">
          <table className="assignments-table">
            <thead>
              <tr>
                <th>Date Time</th>
                <th>Status</th>
                <th>In-Time</th>
                <th>Out-Time</th>
                <th>Updated By</th>
              </tr>
            </thead>
            <tbody>
              {attendance.length > 0 ? (
                attendance.map((row, idx) => (
                  <tr key={`${row.record_id || row.worker_ref || 'row'}-${idx}`}>
                    <td>{attendanceWhen(row) ? new Date(attendanceWhen(row)).toLocaleString() : '-'}</td>
                    <td>{attendanceStatusText(row)}</td>
                    <td>{formatAttendanceTime(attendanceInTime(row), '-')}</td>
                    <td>{formatAttendanceTime(attendanceOutTime(row), 'Active Session')}</td>
                    <td>{row.updated_by || row.source || '-'}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan="5" className="empty-state-cell">No attendance records found.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    );
  };

  return (
    <div className="dashboard-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand__icon">
            <Cpu size={20} />
          </div>
          <span className="brand__name">SmartSched</span>
        </div>
        <nav className="sidebar__nav">
          {navItems.map((item) => (
            <NavItem
              key={item.key}
              icon={<item.icon size={18} />}
              label={item.label}
              active={activeSection === item.key}
              onClick={() => setActiveSection(item.key)}
            />
          ))}
        </nav>
        <div className="sidebar__footer">
          <NavItem
            icon={<LogOut size={18} />}
            label="Logout"
            onClick={async () => {
              try { await client.post('/logout'); } catch (error) { console.error(error); }
              sessionStorage.clear();
              window.location.reload();
            }}
          />
        </div>
      </aside>
      <main className="dashboard-main">
        {message && <div className="flash-msg">{message}</div>}
        {renderSection()}
      </main>
    </div>
  );
};

const NavItem = ({ icon, label, active = false, onClick }) => (
  <button type="button" onClick={onClick} className={`nav-item ${active ? 'nav-item--active' : ''}`}>
    {icon}
    <span>{label}</span>
  </button>
);

const StatCard = ({ label, value, trend, icon, color }) => {
  const toneClass = {
    blue: 'stat-card__icon--blue',
    emerald: 'stat-card__icon--emerald',
    amber: 'stat-card__icon--amber',
    indigo: 'stat-card__icon--indigo',
  };
  return (
    <article className="stat-card">
      <div className="stat-card__top">
        <div className={`stat-card__icon ${toneClass[color]}`}>{icon}</div>
        <span className="stat-card__trend">{trend}</span>
      </div>
      <div className="stat-card__content">
        <p className="stat-card__label">{label}</p>
        <p className="stat-card__value">{value}</p>
      </div>
    </article>
  );
};

export default WorkerDashboard;

