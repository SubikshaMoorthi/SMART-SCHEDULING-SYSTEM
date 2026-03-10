import React, { useState } from 'react';
import axios from 'axios';

const Login = ({ onLoginSuccess }) => {
    const [formData, setFormData] = useState({ username: '', password: '' });
    const [error, setError] = useState('');

    const styles = {
        wrapper: { height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', backgroundColor: '#061a2f' },
        card: { backgroundColor: 'white', padding: '40px', borderRadius: '16px', width: '100%', maxWidth: '400px', textAlign: 'center' },
        input: { width: '100%', padding: '12px', margin: '10px 0', borderRadius: '8px', border: '1px solid #a8b5c6' },
        button: { width: '100%', padding: '12px', backgroundColor: '#4b6484', color: 'white', border: 'none', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }
    };

    const handleLogin = async (e) => {
        e.preventDefault();
        const data = new FormData();
        data.append('username', formData.username);
        data.append('password', formData.password);

        try {
            const res = await axios.post('http://127.0.0.1:8000/login', data);
            sessionStorage.setItem('token', res.data.access_token);
            sessionStorage.setItem('role', res.data.role);
            onLoginSuccess(res.data.role);
        } catch {
            setError("Invalid username or password");
        }
    };

    return (
        <div style={styles.wrapper}>
            <div style={styles.card}>
                <h2 style={{ color: '#1f2f4a' }}>SmartSched Login</h2>
                {error && <p style={{ color: 'red', fontSize: '14px' }}>{error}</p>}
                <form onSubmit={handleLogin}>
                    <input style={styles.input} type="text" placeholder="Username" onChange={(e) => setFormData({ ...formData, username: e.target.value })} />
                    <input style={styles.input} type="password" placeholder="Password" onChange={(e) => setFormData({ ...formData, password: e.target.value })} />
                    <button style={styles.button} type="submit">Sign In</button>
                </form>
            </div>
        </div>
    );
};

export default Login;

