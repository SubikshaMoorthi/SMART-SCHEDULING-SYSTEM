import React, { useCallback, useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Briefcase,
  CalendarClock,
  Cpu,
  LayoutDashboard,
  LogOut,
  Play,
  RefreshCcw,
  UserCheck,
  Users,
} from 'lucide-react';
import MachineTimeline from '../components/MachineTimeline';
import '../App.css';

const API = 'http://127.0.0.1:8000';
const DEFAULT_SKILL_OPTIONS = ['CNC', 'Welding', 'Assembly', 'Painting', 'Quality'];
const DEFAULT_PURPOSE_OPTIONS = ['CNC', 'Welding', 'Assembly', 'Painting', 'Quality', 'Maintenance', 'Production'];
const DEFAULT_DEPARTMENT_OPTIONS = ['CNC', 'Welding', 'Assembly', 'Painting', 'Quality', 'Maintenance', 'Production'];

const navItems = [
  { key: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { key: 'jobs', label: 'Jobs', icon: Briefcase },
  { key: 'machines', label: 'Machines', icon: Cpu },
  { key: 'users', label: 'Users', icon: Users },
  { key: 'attendance', label: 'Attendance', icon: UserCheck },
  { key: 'scheduling', label: 'Scheduling', icon: CalendarClock },
  { key: 'reports', label: 'Reports/Analytics', icon: BarChart3 },
];

const apiClient = () => {
  const token = sessionStorage.getItem('token');
  return axios.create({
    baseURL: API,
    headers: { Authorization: `Bearer ${token}` },
  });
};

const defaultJob = {
  job_id: '',
  job_name: '',
  processing_time: 60,
  due_date: '',
  priority: 5,
  required_skill: '',
  required_machine_purpose: '',
  created_by: 'Admin',
};

const defaultMachine = {
  machine_id: '',
  machine_name: '',
  purpose: '',
  status: 'Available',
};

const defaultUser = {
  user_id: '',
  name: '',
  role: 'worker',
  skills: '',
  department: '',
  shift: 'Day',
  leave_status: 'Present',
};

const uniqueSorted = (items = []) => [...new Set((items || []).filter(Boolean).map((x) => String(x).trim()))].sort();

const AdminDashboard = () => {
  const [activeSection, setActiveSection] = useState('dashboard');
  const [dashboard, setDashboard] = useState({});
  const [jobs, setJobs] = useState([]);
  const [machines, setMachines] = useState([]);
  const [users, setUsers] = useState([]);
  const [attendance, setAttendance] = useState([]);
  const [schedule, setSchedule] = useState([]);
  const [reports, setReports] = useState({ gantt: [] });
  const [options, setOptions] = useState({
    skills: DEFAULT_SKILL_OPTIONS,
    departments: DEFAULT_DEPARTMENT_OPTIONS,
    machine_purposes: DEFAULT_PURPOSE_OPTIONS,
  });
  const [attendanceRecords, setAttendanceRecords] = useState([]);

  const [jobForm, setJobForm] = useState(defaultJob);
  const [machineForm, setMachineForm] = useState(defaultMachine);
  const [userForm, setUserForm] = useState(defaultUser);
  const [loadingSchedule, setLoadingSchedule] = useState(false);
  const [message, setMessage] = useState('');

  const client = useMemo(() => apiClient(), []);

  const setNotice = (text) => {
    setMessage(text);
    window.setTimeout(() => setMessage(''), 3000);
  };

  const loadAll = useCallback(async () => {
    try {
      const [d, j, m, u, a, s, r] = await Promise.all([
        client.get('/admin/dashboard'),
        client.get('/admin/jobs'),
        client.get('/admin/machines'),
        client.get('/admin/users'),
        client.get('/admin/attendance'),
        client.get('/admin/schedule'),
        client.get('/admin/reports'),
      ]);
      const [opt, records] = await Promise.all([
        client.get('/admin/options'),
        client.get('/admin/attendance-records'),
      ]);
      setDashboard(d.data);
      setJobs(j.data);
      setMachines(m.data);
      setUsers(u.data);
      setAttendance(a.data);
      setSchedule(s.data);
      setReports(r.data);
      const optionPayload = opt.data || {};
      setOptions({
        skills: uniqueSorted([...(optionPayload.skills || []), ...DEFAULT_SKILL_OPTIONS]),
        departments: uniqueSorted([...(optionPayload.departments || []), ...DEFAULT_DEPARTMENT_OPTIONS]),
        machine_purposes: uniqueSorted([...(optionPayload.machine_purposes || []), ...DEFAULT_PURPOSE_OPTIONS]),
      });
      setAttendanceRecords(records.data || []);
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Failed to load admin data');
    }
  }, [client]);

  useEffect(() => {
    loadAll();
  }, [loadAll]);

  const handleCreateJob = async (e) => {
    e.preventDefault();
    try {
      await client.post('/admin/jobs', {
        ...jobForm,
        processing_time: Number(jobForm.processing_time),
        priority: Number(jobForm.priority),
      });
      setJobForm(defaultJob);
      setNotice('Job created');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to create job');
    }
  };

  const handleCreateMachine = async (e) => {
    e.preventDefault();
    try {
      await client.post('/admin/machines', machineForm);
      setMachineForm(defaultMachine);
      setNotice('Machine added');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to add machine');
    }
  };

  const handleMachineStatus = async (machineId, status) => {
    try {
      await client.patch(`/admin/machines/${machineId}/status`, { status });
      setNotice('Machine status updated');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to update machine status');
    }
  };

  const handleCreateUser = async (e) => {
    e.preventDefault();
    try {
      await client.post('/admin/users', {
        ...userForm,
        skills: userForm.role === 'worker' ? userForm.skills : null,
        department: userForm.role === 'supervisor' ? userForm.department : null,
      });
      setUserForm(defaultUser);
      setNotice('User added');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to add user');
    }
  };

  const handleDeleteUser = async (userId) => {
    try {
      await client.delete(`/admin/users/${userId}`);
      setNotice('User deleted');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to delete user');
    }
  };

  const handleAttendance = async (workerId, nextLeaveStatus) => {
    const worker = attendance.find((row) => row.user_id === workerId);
    if (!worker) return;
    try {
      await client.put(`/admin/attendance/${workerId}`, {
        shift: worker.shift_name || 'Day',
        skills: worker.skills || '',
        leave_status: nextLeaveStatus,
        in_time: String(worker.in_time).slice(0, 5),
        out_time: String(worker.out_time).slice(0, 5),
      });
      setNotice('Attendance updated');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to update attendance');
    }
  };

  const handleGenerateSchedule = async () => {
    setLoadingSchedule(true);
    try {
      await client.post('/admin/generate-schedule');
      setNotice('Schedule generated');
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to generate schedule');
    } finally {
      setLoadingSchedule(false);
    }
  };

  const handleReschedule = async (type) => {
    try {
      await client.post('/admin/reschedule', { type });
      setNotice(`Dynamic rescheduling executed: ${type}`);
      loadAll();
    } catch (error) {
      setNotice(error?.response?.data?.detail || 'Unable to reschedule');
    }
  };

  const renderSection = () => {
    if (activeSection === 'dashboard') {
      return (
        <>
          <header className="hero">
            <div className="hero__text">
              <p className="hero__eyebrow">Admin Command Center</p>
              <h1 className="hero__title">Operations Overview</h1>
              <p className="hero__subtitle">Full control of jobs, resources, attendance, and scheduling.</p>
            </div>
            <button onClick={handleGenerateSchedule} disabled={loadingSchedule} className="generate-btn">
              {loadingSchedule ? <span className="spinner" /> : <><Play size={18} />Generate Schedule</>}
            </button>
          </header>

          <div className="stats-grid">
            <StatCard label="Total Jobs" value={dashboard.total_jobs ?? 0} trend="Admin View" icon={<Briefcase />} color="blue" />
            <StatCard label="Pending Jobs" value={dashboard.pending_jobs ?? 0} trend="Priority Queue" icon={<AlertTriangle />} color="amber" />
            <StatCard label="Available Machines" value={dashboard.active_machines ?? 0} trend="Live" icon={<Cpu />} color="emerald" />
            <StatCard label="Workers" value={dashboard.total_workers ?? 0} trend="Skilled Pool" icon={<Users />} color="indigo" />
          </div>

          <ScheduleTable rows={schedule} />
        </>
      );
    }

    if (activeSection === 'jobs') {
      return (
        <section className="admin-grid">
          <form className="admin-form" onSubmit={handleCreateJob}>
            <h2>Create Job</h2>
            <input placeholder="Job ID" value={jobForm.job_id} onChange={(e) => setJobForm({ ...jobForm, job_id: e.target.value })} />
            <input placeholder="Job Name" value={jobForm.job_name} onChange={(e) => setJobForm({ ...jobForm, job_name: e.target.value })} required />
            <input type="number" min="1" placeholder="Processing Time (minutes)" value={jobForm.processing_time} onChange={(e) => setJobForm({ ...jobForm, processing_time: e.target.value })} required />
            <input type="datetime-local" value={jobForm.due_date} onChange={(e) => setJobForm({ ...jobForm, due_date: e.target.value })} required />
            <input type="number" min="1" max="10" placeholder="Priority (1-10)" value={jobForm.priority} onChange={(e) => setJobForm({ ...jobForm, priority: e.target.value })} required />
            <select value={jobForm.required_skill} onChange={(e) => setJobForm({ ...jobForm, required_skill: e.target.value })} required>
              <option value="" disabled>Select Required Skill</option>
              {(options.skills || []).map((skill) => <option key={skill} value={skill}>{skill}</option>)}
            </select>
            <select value={jobForm.required_machine_purpose} onChange={(e) => setJobForm({ ...jobForm, required_machine_purpose: e.target.value })} required>
              <option value="" disabled>Select Machine Purpose</option>
              {(options.machine_purposes || []).map((purpose) => <option key={purpose} value={purpose}>{purpose}</option>)}
            </select>
            <button type="submit">Create Job</button>
          </form>

          <TablePanel title="Jobs">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>Job ID</th>
                  <th>Name</th>
                  <th>Priority</th>
                  <th>Due Date</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.id}>
                    <td>{job.job_id}</td>
                    <td>{job.job_name}</td>
                    <td>{job.priority}</td>
                    <td>{new Date(job.due_date).toLocaleString()}</td>
                    <td>{job.status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TablePanel>
        </section>
      );
    }

    if (activeSection === 'machines') {
      return (
        <section className="admin-grid">
          <form className="admin-form" onSubmit={handleCreateMachine}>
            <h2>Add Machine</h2>
            <input placeholder="Machine ID" value={machineForm.machine_id} onChange={(e) => setMachineForm({ ...machineForm, machine_id: e.target.value })} />
            <input placeholder="Machine Name" value={machineForm.machine_name} onChange={(e) => setMachineForm({ ...machineForm, machine_name: e.target.value })} required />
            <select value={machineForm.purpose} onChange={(e) => setMachineForm({ ...machineForm, purpose: e.target.value })} required>
              <option value="" disabled>Select Machine Purpose</option>
              {(options.machine_purposes || []).map((purpose) => <option key={purpose} value={purpose}>{purpose}</option>)}
            </select>
            <select value={machineForm.status} onChange={(e) => setMachineForm({ ...machineForm, status: e.target.value })}>
              <option>Available</option>
              <option>Busy</option>
              <option>Under Maintenance</option>
              <option>Breakdown</option>
            </select>
            <button type="submit">Add Machine</button>
          </form>

          <TablePanel title="Machines">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>Machine ID</th>
                  <th>Name</th>
                  <th>Purpose</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {machines.map((machine) => (
                  <tr key={machine.id}>
                    <td>{machine.machine_id}</td>
                    <td>{machine.machine_name}</td>
                    <td>{machine.purpose}</td>
                    <td>
                      <select
                        value={machine.status}
                        onChange={(e) => handleMachineStatus(machine.machine_id, e.target.value)}
                      >
                        <option>Available</option>
                        <option>Busy</option>
                        <option>Under Maintenance</option>
                        <option>Breakdown</option>
                      </select>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TablePanel>
        </section>
      );
    }

    if (activeSection === 'users') {
      return (
        <section className="admin-grid">
          <form className="admin-form" onSubmit={handleCreateUser}>
            <h2>Add User</h2>
            <input placeholder="User ID" value={userForm.user_id} onChange={(e) => setUserForm({ ...userForm, user_id: e.target.value })} />
            <input placeholder="Name" value={userForm.name} onChange={(e) => setUserForm({ ...userForm, name: e.target.value })} required />
            <select value={userForm.role} onChange={(e) => setUserForm({ ...userForm, role: e.target.value })}>
              <option value="worker">Worker</option>
              <option value="supervisor">Supervisor</option>
            </select>
            {userForm.role === 'worker' && (
              <select value={userForm.skills} onChange={(e) => setUserForm({ ...userForm, skills: e.target.value })} required>
                <option value="" disabled>Select Skill</option>
                {(options.skills || []).map((skill) => <option key={skill} value={skill}>{skill}</option>)}
              </select>
            )}
            {userForm.role === 'supervisor' && (
              <select value={userForm.department} onChange={(e) => setUserForm({ ...userForm, department: e.target.value })} required>
                <option value="" disabled>Select Department</option>
                {(options.departments || []).map((dept) => <option key={dept} value={dept}>{dept}</option>)}
              </select>
            )}
            <input placeholder="Shift" value={userForm.shift} onChange={(e) => setUserForm({ ...userForm, shift: e.target.value })} />
            <button type="submit">Add User</button>
          </form>

          <TablePanel title="Users">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>User ID</th>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Skills</th>
                  <th>Department</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {users.map((user) => (
                  <tr key={`${user.id}-${user.user_id}`}>
                    <td>{user.user_id}</td>
                    <td>{user.name}</td>
                    <td>{user.role}</td>
                    <td>{user.skills || '-'}</td>
                    <td>{user.department || '-'}</td>
                    <td>
                      <button className="mini-btn" onClick={() => handleDeleteUser(user.user_id)}>
                        Delete
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TablePanel>
        </section>
      );
    }

    if (activeSection === 'attendance') {
      return (
        <>
          <TablePanel title="Attendance Status (Workers & Supervisors)">
            <table className="assignments-table">
              <thead>
                <tr>
                  <th>User ID</th>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Shift</th>
                  <th>Skills / Department</th>
                  <th>Status</th>
                  <th>In-Time</th>
                  <th>Out-Time</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {attendance.map((worker) => (
                  <tr key={`${worker.role}-${worker.user_id}-${worker.id}`}>
                    <td>{worker.user_id}</td>
                    <td>{worker.name}</td>
                    <td>{worker.role}</td>
                    <td>{worker.shift_name || 'Day'}</td>
                    <td>{worker.skills || worker.department || '-'}</td>
                    <td>{worker.leave_status}</td>
                    <td>{String(worker.in_time).slice(0, 5)}</td>
                    <td>{String(worker.out_time).slice(0, 5)}</td>
                    <td>
                      {worker.role === 'worker' ? (
                        <button
                          className="mini-btn"
                          onClick={() => handleAttendance(worker.user_id, worker.leave_status === 'Present' ? 'On Leave' : 'Present')}
                        >
                          Toggle Leave
                        </button>
                      ) : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TablePanel>
          <TablePanel title="Login / Logout Records (All Users)">
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
                {attendanceRecords.map((row) => (
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
          </TablePanel>
        </>
      );
    }

    if (activeSection === 'scheduling') {
      return (
        <>
          <div className="action-row">
            <button className="generate-btn" onClick={handleGenerateSchedule} disabled={loadingSchedule}>
              {loadingSchedule ? <span className="spinner" /> : <><Play size={18} />Generate Schedule</>}
            </button>
            <button className="ghost-btn" onClick={() => handleReschedule('high_priority_job')}>
              <RefreshCcw size={16} /> High Priority Reschedule
            </button>
            <button className="ghost-btn" onClick={() => handleReschedule('machine_breakdown')}>
              <AlertTriangle size={16} /> Machine Breakdown Reschedule
            </button>
            <button className="ghost-btn" onClick={() => handleReschedule('worker_absence')}>
              <Activity size={16} /> Worker Absence Reschedule
            </button>
          </div>
          <ScheduleTable rows={schedule} />
        </>
      );
    }

    return (
      <TablePanel title="Reports / Analytics">
        <div className="reports-kpi">
          <div className="kpi">Jobs Scheduled: {reports.jobs_scheduled ?? 0}</div>
          <div className="kpi">Machine Busy Minutes: {reports.machine_busy_minutes ?? 0}</div>
          <div className="kpi">Machines: {reports.machine_count ?? 0}</div>
          <div className="kpi">Workers: {reports.worker_count ?? 0}</div>
        </div>
        <MachineTimeline rows={reports.gantt || []} className="timeline-admin" />
      </TablePanel>
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

const TablePanel = ({ title, children }) => (
  <section className="assignments-panel">
    <div className="assignments-panel__header">
      <h2>{title}</h2>
    </div>
    <div className="table-wrap">{children}</div>
  </section>
);

const ScheduleTable = ({ rows }) => (
  <TablePanel title="Schedule / Allocation">
    <table className="assignments-table">
      <thead>
        <tr>
          <th>Job</th>
          <th>Worker</th>
          <th>Machine</th>
          <th>Start</th>
          <th>End</th>
          <th>Reason</th>
        </tr>
      </thead>
      <tbody>
        {rows.length > 0 ? (
          rows.map((item) => (
            <tr key={item.id}>
              <td>{item.job_id} - {item.job_name}</td>
              <td>{item.worker_name}</td>
              <td>{item.machine_name}</td>
              <td>{new Date(item.start_time).toLocaleString()}</td>
              <td>{new Date(item.end_time).toLocaleString()}</td>
              <td>{item.reason || 'Normal'}</td>
            </tr>
          ))
        ) : (
          <tr>
            <td colSpan="6" className="empty-state-cell">No schedule generated yet.</td>
          </tr>
        )}
      </tbody>
    </table>
  </TablePanel>
);

export default AdminDashboard;

