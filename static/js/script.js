(() => {
    const symbolSelect = document.getElementById('symbolSelect');
    const spotPriceEl  = document.getElementById('spotPrice');
    const expiryLabel  = document.getElementById('expiryLabel');
    const chainBody    = document.getElementById('chainBody');

    const fmt  = (n) => n != null ? new Intl.NumberFormat('en-IN').format(n) : '—';
    const fmtP = (n) => n != null ? n.toFixed(2) : '—';
    const changeSign = (v) => v > 0 ? '+' + fmtP(v) : fmtP(v);

    // Find max values across all rows for CE and PE independently
    function findMaxes(chain) {
        const maxes = {
            CE: { volume: -Infinity, oi: -Infinity, oi_change: -Infinity },
            PE: { volume: -Infinity, oi: -Infinity, oi_change: -Infinity },
        };
        chain.forEach(row => {
            ['CE', 'PE'].forEach(side => {
                const d = row[side];
                if (!d) return;
                if (d.volume    > maxes[side].volume)    maxes[side].volume    = d.volume;
                if (d.oi        > maxes[side].oi)        maxes[side].oi        = d.oi;
                if (Math.abs(d.oi_change) > Math.abs(maxes[side].oi_change))
                    maxes[side].oi_change = d.oi_change;
            });
        });
        return maxes;
    }

    function renderChain(data) {
        spotPriceEl.textContent = '₹' + fmt(data.spot_price);
        expiryLabel.textContent = `Expiry: ${data.expiry}`;

        const maxes = findMaxes(data.chain);

        let html = '';
        data.chain.forEach(row => {
            const isAtm  = row.strike === data.atm_strike;
            const ceItm  = row.strike < data.atm_strike;
            const peItm  = row.strike > data.atm_strike;
            const ce = row.CE;
            const pe = row.PE;

            // Helper: build a <td> with base class, optional itm, optional highlight
            function td(side, field, content, extraCls = '') {
                const d = side === 'CE' ? ce : pe;
                const baseItm = side === 'CE' ? (ceItm ? 'ce-itm' : '') : (peItm ? 'pe-itm' : '');
                const isMax = d && maxes[side][field] !== -Infinity && (
                    field === 'oi_change'
                        ? Math.abs(d[field]) === Math.abs(maxes[side][field])
                        : d[field] === maxes[side][field]
                );
                const hlClass = isMax ? (side === 'CE' ? 'hl-ce' : 'hl-pe') : '';
                const cls = [baseItm, hlClass, extraCls].filter(Boolean).join(' ');
                return `<td class="${cls}">${content}</td>`;
            }

            html += `<tr${isAtm ? ' class="atm-row"' : ''}>`;

            // CALLS: Volume | OI | Chg OI | LTP
            html += td('CE', 'volume',    ce ? fmt(ce.volume)              : '—');
            html += td('CE', 'oi',        ce ? fmt(ce.oi)                  : '—');
            html += td('CE', 'oi_change', ce ? changeSign(ce.oi_change)    : '—',
                       ce ? (ce.oi_change >= 0 ? 'oi-change up' : 'oi-change dn') : '');
            html += `<td class="${ceItm ? 'ce-itm' : ''} ltp">${ce ? fmtP(ce.ltp) : '—'}</td>`;

            // STRIKE
            html += `<td class="strike-cell">${fmt(row.strike)}</td>`;

            // PUTS: LTP | Chg OI | OI | Volume
            html += `<td class="${peItm ? 'pe-itm' : ''} ltp">${pe ? fmtP(pe.ltp) : '—'}</td>`;
            html += td('PE', 'oi_change', pe ? changeSign(pe.oi_change)    : '—',
                       pe ? (pe.oi_change >= 0 ? 'oi-change up' : 'oi-change dn') : '');
            html += td('PE', 'oi',        pe ? fmt(pe.oi)                  : '—');
            html += td('PE', 'volume',    pe ? fmt(pe.volume)              : '—');

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
