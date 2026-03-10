import React, { useState } from 'react';
import AdminDashboard from './pages/AdminDashboard';
import Login from './pages/Login';
import SupervisorDashboard from './pages/SupervisorDashboard';
import WorkerDashboard from './pages/WorkerDashboard';

const App = () => {
    const [userRole, setUserRole] = useState(sessionStorage.getItem('role'));

    if (!userRole) {
        return <Login onLoginSuccess={(role) => setUserRole(role)} />;
    }

    if (userRole === 'admin') return <AdminDashboard />;
    if (userRole === 'supervisor') return <SupervisorDashboard />;
    if (userRole === 'worker') return <WorkerDashboard />;
    return <h1>Unknown role</h1>;
};

export default App;

