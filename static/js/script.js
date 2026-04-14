document.addEventListener('DOMContentLoaded', () => {
    const symbolSelect = document.getElementById('symbolSelect');
    const spotPriceEl = document.getElementById('spotPrice');
    const expiryDateEl = document.getElementById('expiryDate');
    const chainBody = document.getElementById('chainBody');
    
    let intervalId;

    const formatNumber = (num) => {
        if (num === null || num === undefined) return '-';
        return new Intl.NumberFormat('en-IN').format(num);
    };

    const formatPrice = (num) => {
        if (num === null || num === undefined) return '-';
        return new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR' }).format(num);
    };

    const fetchOptionChain = async () => {
        const symbol = symbolSelect.value;
        try {
            const res = await fetch(`/api/option-chain?symbol=${symbol}`);
            if (res.status === 401) {
                // Session expired or no access token
                window.location.href = '/login';
                return;
            }
            const data = await res.json();
            
            if (data.success) {
                renderChain(data);
            } else {
                console.error('Error fetching data:', data.error);
                if (chainBody.innerHTML.includes('Loading live market data')) {
                    chainBody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: #ef4444;">Error: ${data.error}</td></tr>`;
                }
            }
        } catch (err) {
            console.error('Network error:', err);
        }
    };

    const renderChain = (data) => {
        spotPriceEl.textContent = formatPrice(data.spot_price);
        expiryDateEl.textContent = data.expiry;
        chainBody.innerHTML = '';

        data.chain.forEach(row => {
            const tr = document.createElement('tr');
            
            // Determine if row is ATM (closest to spot)
            if (row.strike === data.atm_strike) {
                tr.classList.add('atm-row');
            }

            // CE ITM calculation (strike < spot)
            const ceItmClass = row.strike < data.atm_strike ? 'ce-itm' : '';
            // PE ITM calculation (strike > spot)
            const peItmClass = row.strike > data.atm_strike ? 'pe-itm' : '';

            // Formatting helper
            const getVal = (side, key) => {
                if (!row[side]) return '-';
                const v = row[side][key];
                if (key === 'ltp' || key === 'oi_change_pct') {
                     // For UI simplicity we just show number not currency for LTP in exact option chain standard
                     return v !== undefined ? v.toFixed(2) : '-';
                }
                return formatNumber(v);
            };

            const ceOiChangeStr = row.CE && row.CE.oi_change_pct !== undefined 
                ? `<span style="color: ${row.CE.oi_change_pct > 0 ? '#4ade80' : '#ef4444'}">${(row.CE.oi_change_pct).toFixed(2)}</span>`
                : '-';
                
            const peOiChangeStr = row.PE && row.PE.oi_change_pct !== undefined 
                ? `<span style="color: ${row.PE.oi_change_pct > 0 ? '#4ade80' : '#ef4444'}">${(row.PE.oi_change_pct).toFixed(2)}</span>`
                : '-';

            tr.innerHTML = `
                <td class="${ceItmClass}">${getVal('CE', 'volume')}</td>
                <td class="${ceItmClass}">${getVal('CE', 'oi')}</td>
                <td class="${ceItmClass}" style="font-weight: 600;">${getVal('CE', 'ltp')}</td>
                <td class="mid-col">${formatNumber(row.strike)}</td>
                <td class="${peItmClass}" style="font-weight: 600;">${getVal('PE', 'ltp')}</td>
                <td class="${peItmClass}">${getVal('PE', 'oi')}</td>
                <td class="${peItmClass}">${getVal('PE', 'volume')}</td>
            `;
            chainBody.appendChild(tr);
        });
    };

    // Listeners
    symbolSelect.addEventListener('change', () => {
        chainBody.innerHTML = `<tr><td colspan="7" style="text-align: center; padding: 2rem;">Loading live market data...</td></tr>`;
        fetchOptionChain();
    });

    // Start polling
    fetchOptionChain();
    intervalId = setInterval(fetchOptionChain, 5000);
});
