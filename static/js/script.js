(() => {
    const symbolSelect = document.getElementById('symbolSelect');
    const spotPriceEl  = document.getElementById('spotPrice');
    const expiryLabel  = document.getElementById('expiryLabel');
    const chainBody    = document.getElementById('chainBody');

    const fmt  = (n) => n != null ? new Intl.NumberFormat('en-IN').format(n) : '—';
    const fmtP = (n) => n != null ? n.toFixed(2) : '—';

    const changeClass = (v) => v > 0 ? 'oi-change up' : v < 0 ? 'oi-change dn' : '';
    const changeSign  = (v) => v > 0 ? '+' + fmtP(v) : fmtP(v);

    function cell(cls, content) {
        return `<td class="${cls}">${content}</td>`;
    }

    function renderChain(data) {
        spotPriceEl.textContent = '₹' + fmt(data.spot_price);
        expiryLabel.textContent = `Expiry: ${data.expiry}`;

        let html = '';
        data.chain.forEach(row => {
            const isAtm = row.strike === data.atm_strike;
            const ceItm = row.strike < data.atm_strike;
            const peItm = row.strike > data.atm_strike;

            const ce = row.CE;
            const pe = row.PE;

            const ceCls = ceItm ? 'ce-itm' : '';
            const peCls = peItm ? 'pe-itm' : '';

            html += `<tr${isAtm ? ' class="atm-row"' : ''}>`;
            // CALLS (right-to-left mirror of NSE: Vol, OI, ChgOI, LTP)
            html += cell(ceCls, ce ? fmt(ce.volume) : '—');
            html += cell(ceCls, ce ? fmt(ce.oi) : '—');
            html += cell(ceCls + (ce ? ' ' + changeClass(ce.oi_change) : ''), ce ? changeSign(ce.oi_change) : '—');
            html += cell(ceCls + ' ltp', ce ? fmtP(ce.ltp) : '—');
            // STRIKE
            html += `<td class="strike-cell">${fmt(row.strike)}</td>`;
            // PUTS (LTP, ChgOI, OI, Vol)
            html += cell(peCls + ' ltp', pe ? fmtP(pe.ltp) : '—');
            html += cell(peCls + (pe ? ' ' + changeClass(pe.oi_change) : ''), pe ? changeSign(pe.oi_change) : '—');
            html += cell(peCls, pe ? fmt(pe.oi) : '—');
            html += cell(peCls, pe ? fmt(pe.volume) : '—');
            html += '</tr>';
        });

        chainBody.innerHTML = html;
    }

    async function fetchData() {
        const symbol = symbolSelect.value;
        try {
            const res = await fetch(`/api/option-chain?symbol=${symbol}`);
            if (res.status === 401) { window.location.href = '/'; return; }
            const data = await res.json();
            if (data.success) {
                renderChain(data);
                document.getElementById('liveBadge').style.opacity = '1';
            } else {
                console.error('API error:', data.error);
            }
        } catch (err) {
            console.error('Fetch error:', err);
            document.getElementById('liveBadge').style.opacity = '0.4';
        }
    }

    symbolSelect.addEventListener('change', () => {
        chainBody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:2.5rem;color:var(--muted)">Loading…</td></tr>';
        fetchData();
    });

    fetchData();
    setInterval(fetchData, 5000);
})();
