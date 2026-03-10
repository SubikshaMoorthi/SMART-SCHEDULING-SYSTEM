import React, { useCallback, useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  AlertTriangle,
  BarChart3,
  Briefcase,
  CalendarClock,
  Cpu,
  LayoutDashboard,
  LogOut,
  UserCheck,
  Users,
} from 'lucide-react';
import MachineTimeline from '../components/MachineTimeline';
import '../App.css';

const API = 'http://127.0.0.1:8000';

const navItems = [
  { key: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { key: 'workers', label: 'Workers', icon: Users },
  { key: 'jobs', label: 'Jobs', icon: Briefcase },
  { key: 'machines', label: 'Machines', icon: Cpu },
  { key: 'schedule', label: 'Schedule / Gantt', icon: CalendarClock },
  { key: 'attendance', label: 'Attendance', icon: UserCheck },
];

const apiClient = () => {
  const token = sessionStorage.getItem('token');
  return axios.create({
    baseURL: API,
    headers: { Authorization: `Bearer ${token}` },
  });
};

const SupervisorDashboard = () => {
  const [activeSection, setActiveSection] = useState('dashboard');
  const [profile, setProfile] = useState({});
  const [dashboard, setDashboard] = useState({});
  const [workers, setWorkers] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [machines, setMachines] = useState([]);
  const [schedule, setSchedule] = useState([]);
  const [attendance, setAttendance] = useState([]);
  const [attendanceStatus, setAttendanceStatus] = useState({ active_session: false, leave_status: 'On Leave' });
  const [message, setMessage] = useState('');

  const client = useMemo(() => apiClient(), []);

  const setNotice = (text) => {
    setMessage(text);
    window.setTimeout(() => setMessage(''), 3000);
  };

  const loadAll = useCallback(async () => {
    try {
      const [p, d, w, j, m, s, a] = await Promise.all([
        client.get('/supervisor/profile'),
        client.get('/supervisor/dashboard'),
        client.get('/supervisor/workers'),
        client.get('/supervisor/jobs'),
        client.get('/supervisor/machines'),
        client.get('/supervisor/schedule'),
        client.get('/supervisor/attendance-history'),
      ]);
      const status = await client.get('/supervisor/attendance-status');
      setProfile(p.data);
      setDashboard(d.data);
      setWorkers(w.data);
      setJobs(j.data);
      setMachines(m.data);
      setSchedule(s.data);
      setAttendance(a.data);
      setAttendanceStatus(status.data || { active_session: false, leave_status: 'On Leave' });
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Failed to load supervisor data');
    }
  }, [client]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      loadAll();
    }, 0);
    return () => window.clearTimeout(timer);
  }, [loadAll]);

  const reportWorkerAbsence = async (workerId) => {
    try {
      await client.post('/supervisor/report-worker-absence', { worker_id: workerId });
      setNotice('Worker absence reported. Rescheduling triggered.');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Could not report worker absence');
    }
  };

  const reportMachineBreakdown = async (machineId) => {
    try {
      await client.post('/supervisor/report-machine-breakdown', { machine_id: machineId });
      setNotice('Machine breakdown reported. Rescheduling triggered.');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Could not report machine breakdown');
    }
  };

  const checkIn = async () => {
    try {
      await client.post('/supervisor/attendance/in');
      setNotice('Attendance started');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to check in');
    }
  };

  const checkOut = async () => {
    try {
      await client.post('/supervisor/attendance/out');
      setNotice('Attendance closed');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to check out');
    }
  };

  const requestLeave = async () => {
    try {
      await client.post('/supervisor/attendance/leave', { reason: 'Requested by supervisor' });
      setNotice('Leave requested');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to request leave');
    }
  };

  const renderSection = () => {
    if (activeSection === 'dashboard') {
      return (
        <>
          <header className="hero">
            <div className="hero__text">
              <p className="hero__eyebrow">Supervisor Panel</p>
              <h1 className="hero__title">Department Monitoring</h1>
              <p className="hero__subtitle">
                Department: <strong>{dashboard.department || profile.department || '-'}</strong>
              </p>
            </div>
          </header>

          <div className="stats-grid">
            <StatCard label="Department Workers" value={dashboard.total_workers ?? 0} trend="Live Team" icon={<Users />} color="blue" />
            <StatCard label="Active Jobs" value={dashboard.active_jobs ?? 0} trend="Scheduled/Running" icon={<Briefcase />} color="emerald" />
            <StatCard label="Completed Jobs" value={dashboard.completed_jobs ?? 0} trend="Output" icon={<UserCheck />} color="indigo" />
            <StatCard label="Delayed Jobs" value={dashboard.delayed_jobs ?? 0} trend="Attention" icon={<AlertTriangle />} color="amber" />
          </div>

          <section className="assignments-panel">
            <div className="assignments-panel__header">
              <h2>Machine Status Overview</h2>
            </div>
            <div className="reports-kpi">
              <div className="kpi">Available: {dashboard.machine_status_overview?.Available ?? 0}</div>
              <div className="kpi">Busy: {dashboard.machine_status_overview?.Busy ?? 0}</div>
              <div className="kpi">Under Maintenance: {dashboard.machine_status_overview?.['Under Maintenance'] ?? 0}</div>
              <div className="kpi">Breakdown: {dashboard.machine_status_overview?.Breakdown ?? 0}</div>
            </div>
          </section>
        </>
      );
    }

    if (activeSection === 'workers') {
      return (
        <section className="assignments-panel">
          <div className="assignments-panel__header">
            <h2>Department Workers</h2>
          </div>
          <div className="table-wrap">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>Worker ID</th>
                  <th>Name</th>
                  <th>Skills</th>
                  <th>Shift</th>
                  <th>Availability</th>
                  <th>Attendance</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {workers.map((w) => (
                  <tr key={`${w.worker_id}-${w.name}`}>
                    <td>{w.worker_id}</td>
                    <td>{w.name}</td>
                    <td>{w.skills}</td>
                    <td>{w.shift} ({w.in_time} - {w.out_time})</td>
                    <td>{w.availability}</td>
                    <td>{w.attendance_status}</td>
                    <td>
                      <button className="mini-btn" onClick={() => reportWorkerAbsence(w.worker_id)}>
                        Report Absence
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      );
    }

    if (activeSection === 'jobs') {
      return (
        <section className="assignments-panel">
          <div className="assignments-panel__header">
            <h2>Department Jobs</h2>
          </div>
          <div className="table-wrap">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>Job ID</th>
                  <th>Job Name</th>
                  <th>Proc Time</th>
                  <th>Due Date</th>
                  <th>Priority</th>
                  <th>Machine</th>
                  <th>Worker</th>
                  <th>Start</th>
                  <th>End</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={`${job.job_id}-${job.start_time || 'none'}`}>
                    <td>{job.job_id}</td>
                    <td>{job.job_name}</td>
                    <td>{job.processing_time} min</td>
                    <td>{job.due_date ? new Date(job.due_date).toLocaleString() : '-'}</td>
                    <td>{job.priority}</td>
                    <td>{job.assigned_machine}</td>
                    <td>{job.assigned_worker}</td>
                    <td>{job.start_time ? new Date(job.start_time).toLocaleString() : '-'}</td>
                    <td>{job.end_time ? new Date(job.end_time).toLocaleString() : '-'}</td>
                    <td>{job.status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      );
    }

    if (activeSection === 'machines') {
      return (
        <section className="assignments-panel">
          <div className="assignments-panel__header">
            <h2>Department Machines</h2>
          </div>
          <div className="table-wrap">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>Machine ID</th>
                  <th>Name</th>
                  <th>Purpose</th>
                  <th>Status</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {machines.map((machine) => (
                  <tr key={`${machine.machine_id}-${machine.machine_name}`}>
                    <td>{machine.machine_id}</td>
                    <td>{machine.machine_name}</td>
                    <td>{machine.purpose}</td>
                    <td>{machine.status}</td>
                    <td>
                      <button className="mini-btn" onClick={() => reportMachineBreakdown(machine.machine_id)}>
                        Report Breakdown
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      );
    }

    if (activeSection === 'attendance') {
      return (
        <section className="assignments-panel">
          <div className="assignments-panel__header">
            <h2>My Attendance</h2>
          </div>
          <div className="action-row">
            {!attendanceStatus.active_session ? (
              <button className="generate-btn" onClick={checkIn}>In</button>
            ) : (
              <button className="generate-btn" onClick={checkOut}>Out</button>
            )}
            <button className="ghost-btn" onClick={requestLeave}>Leave Request</button>
            <div className="kpi">Status: {attendanceStatus.leave_status || '-'}</div>
            <div className="kpi">In-Time: {attendanceStatus.in_time ? new Date(attendanceStatus.in_time).toLocaleString() : '--'}</div>
            <div className="kpi">Auto Out-Time: {attendanceStatus.expected_out_time ? new Date(attendanceStatus.expected_out_time).toLocaleString() : '--'}</div>
          </div>
          <div className="table-wrap">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>Record</th>
                  <th>Username</th>
                  <th>Role</th>
                  <th>Status</th>
                  <th>Source</th>
                  <th>In-Time</th>
                  <th>Out-Time</th>
                </tr>
              </thead>
              <tbody>
                {attendance.map((row) => (
                  <tr key={row.record_id || row.id}>
                    <td>{row.record_id || row.id}</td>
                    <td>{row.username}</td>
                    <td>{row.role}</td>
                    <td>{row.status || '-'}</td>
                    <td>{row.source || '-'}</td>
                    <td>{new Date(row.login_time).toLocaleString()}</td>
                    <td>{row.logout_time ? new Date(row.logout_time).toLocaleString() : 'Active Session'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      );
    }

    return (
      <section className="assignments-panel">
        <div className="assignments-panel__header">
          <h2>Production Schedule & Gantt (Read Only)</h2>
          <span className="live-pill"><BarChart3 size={14} /> View Only</span>
        </div>
        <MachineTimeline rows={schedule} className="timeline-supervisor" />
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

export default SupervisorDashboard;

