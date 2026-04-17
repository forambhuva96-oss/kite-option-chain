(() => {
    const symbolSelect = document.getElementById('symbolSelect');
    const expirySelect = document.getElementById('expirySelect');
    const spotPriceEl  = document.getElementById('spotPrice');
    const expiryLabel  = document.getElementById('expiryLabel');
    const chainBody    = document.getElementById('chainBody');

    const fmt  = (n) => n != null ? new Intl.NumberFormat('en-IN').format(n) : '—';
    const fmtP = (n, d = 2) => (n != null && n !== undefined) ? Number(n).toFixed(d) : '—';
    const chSgn = (v) => {
        if (v == null) return '<span class="na">—</span>';
        const s = v > 0 ? '+' : '';
        return s + new Intl.NumberFormat('en-IN').format(Math.round(v));
    };

    /* ── find top-2 values in ATM + OTM rows only ── */
    function findMaxes(chain, atm_strike) {
        const mx = {
            CE: { volume:[-Infinity,-Infinity], oi:[-Infinity,-Infinity], oi_change:[-Infinity,-Infinity], intraday_oi_chg:[-Infinity,-Infinity] },
            PE: { volume:[-Infinity,-Infinity], oi:[-Infinity,-Infinity], oi_change:[-Infinity,-Infinity], intraday_oi_chg:[-Infinity,-Infinity] },
        };
        function update(arr, val) {
            if (val > arr[0])      { arr[1] = arr[0]; arr[0] = val; }
            else if (val > arr[1]) { arr[1] = val; }
        }
        chain.forEach(r => {
            if (r.strike >= atm_strike && r.CE) {
                update(mx.CE.volume,          r.CE.volume);
                update(mx.CE.oi,              r.CE.oi);
                if (r.CE.oi_change      != null) update(mx.CE.oi_change,      Math.abs(r.CE.oi_change));
                if (r.CE.intraday_oi_chg!= null) update(mx.CE.intraday_oi_chg, Math.abs(r.CE.intraday_oi_chg));
            }
            if (r.strike <= atm_strike && r.PE) {
                update(mx.PE.volume,          r.PE.volume);
                update(mx.PE.oi,              r.PE.oi);
                if (r.PE.oi_change      != null) update(mx.PE.oi_change,      Math.abs(r.PE.oi_change));
                if (r.PE.intraday_oi_chg!= null) update(mx.PE.intraday_oi_chg, Math.abs(r.PE.intraday_oi_chg));
            }
        });
        return mx;
    }

    function td(content, classes = []) {
        const cls = classes.filter(Boolean).join(' ');
        return `<td${cls ? ` class="${cls}"` : ''}>${content}</td>`;
    }

    /* ── OI change cell: shows overnight + intraday stacked ── */
    function oiChangeTd(overnight, intraday, side, itm, mx) {
        const arr = mx[side].oi_change;
        const absO = overnight != null ? Math.abs(overnight) : -Infinity;
        let hlCls = null;
        if (absO !== -Infinity) {
            if (absO === arr[0] && arr[0] !== -Infinity) hlCls = side === 'CE' ? 'hl-ce' : 'hl-pe';
            else if (absO === arr[1] && arr[1] !== -Infinity) hlCls = 'hl-2nd';
        }
        const itmCls = itm ? (side === 'CE' ? 'ce-itm' : 'pe-itm') : null;
        const upDn = v => v == null ? '' : (v >= 0 ? 'oi-up' : 'oi-dn');

        const overnightHtml = `<span class="oi-overnight ${upDn(overnight)}">${chSgn(overnight)}</span>`;
        const intradayHtml  = intraday != null
            ? `<span class="oi-intraday ${upDn(intraday)}" title="Intraday (since 9:15 AM)">${chSgn(intraday)}</span>`
            : '';

        return `<td class="${[itmCls, hlCls, 'oi-chg-cell'].filter(Boolean).join(' ')}">
            <div class="oi-chg-wrap">${overnightHtml}${intradayHtml}</div>
        </td>`;
    }

    /* ── Populate expiry dropdown ── */
    function populateExpiries(expiries, selectedVal) {
        expirySelect.innerHTML = expiries
            .map(e => `<option value="${e.value}"${e.value === selectedVal ? ' selected' : ''}>${e.label}</option>`)
            .join('');
    }

    function renderChain(data) {
        spotPriceEl.textContent = '₹' + fmt(data.spot_price);
        expiryLabel.textContent = `Expiry: ${data.expiry}`;

        if (data.all_expiries && data.all_expiries.length) {
            const firstOption = expirySelect.options[0];
            if (!firstOption || firstOption.value !== data.all_expiries[0].value) {
                populateExpiries(data.all_expiries, data.expiry_val);
            }
        }

        const mx = findMaxes(data.chain, data.atm_strike);
        let html = '';
        data.chain.forEach(row => {
            const isAtm = row.strike === data.atm_strike;
            const ceItm = row.strike < data.atm_strike;
            const peItm = row.strike > data.atm_strike;
            const ce = row.CE, pe = row.PE;

            const hlCls = (side, field, d) => {
                if (!d) return null;
                const arr = mx[side][field];
                const val = field.includes('oi_change') ? Math.abs(d[field]) : d[field];
                if (val == null || val === -Infinity) return null;
                if (val === arr[0] && arr[0] !== -Infinity) return side === 'CE' ? 'hl-ce' : 'hl-pe';
                if (val === arr[1] && arr[1] !== -Infinity) return 'hl-2nd';
                return null;
            };

            html += `<tr${isAtm ? ' class="atm-row"' : ''}>`;

            // CALLS: Greeks | OI | ChgOI | Volume | LTP
            html += td(ce?.iv    != null ? fmtP(ce.iv)       : '—', [ceItm?'ce-itm':null,'greek']);
            html += td(ce?.delta != null ? fmtP(ce.delta, 3) : '—', [ceItm?'ce-itm':null,'greek']);
            html += td(ce?.theta != null ? fmtP(ce.theta)    : '—', [ceItm?'ce-itm':null,'greek']);
            html += td(ce ? fmt(ce.oi) : '—',     [ceItm?'ce-itm':null, hlCls('CE','oi',ce)]);
            // OI change cell (overnight + intraday stacked)
            html += oiChangeTd(ce?.oi_change, ce?.intraday_oi_chg, 'CE', ceItm, mx);
            html += td(ce ? fmt(ce.volume) : '—', [ceItm?'ce-itm':null, hlCls('CE','volume',ce)]);
            html += td(ce ? fmtP(ce.ltp)   : '—', [ceItm?'ce-itm':null,'ltp']);

            // STRIKE
            html += `<td class="strike-cell">${fmt(row.strike)}</td>`;

            // PUTS: LTP | Volume | ChgOI | OI | Greeks
            html += td(pe ? fmtP(pe.ltp)   : '—', [peItm?'pe-itm':null,'ltp']);
            html += td(pe ? fmt(pe.volume) : '—', [peItm?'pe-itm':null, hlCls('PE','volume',pe)]);
            html += oiChangeTd(pe?.oi_change, pe?.intraday_oi_chg, 'PE', peItm, mx);
            html += td(pe ? fmt(pe.oi) : '—',     [peItm?'pe-itm':null, hlCls('PE','oi',pe)]);
            html += td(pe?.iv    != null ? fmtP(pe.iv)       : '—', [peItm?'pe-itm':null,'greek']);
            html += td(pe?.delta != null ? fmtP(pe.delta, 3) : '—', [peItm?'pe-itm':null,'greek']);
            html += td(pe?.theta != null ? fmtP(pe.theta)    : '—', [peItm?'pe-itm':null,'greek']);

            html += '</tr>';
        });

        chainBody.innerHTML = html;
    }

    async function fetchData() {
        const symbol = symbolSelect.value;
        const expiry = expirySelect.value;
        const url    = `/api/option-chain?symbol=${symbol}${expiry ? '&expiry=' + expiry : ''}`;
        try {
            const res  = await fetch(url);
            if (res.status === 401) { window.location.href = '/'; return; }
            const data = await res.json();
            if (data.success) {
                renderChain(data);
                const badge = document.getElementById('liveBadge');
                if (data.is_mock) {
                    badge.textContent = 'Simulated 24/7';
                    badge.style.background = 'rgba(255, 165, 0, 0.15)';
                    badge.style.color = '#ffa500';
                    badge.style.border = '1px solid rgba(255, 165, 0, 0.3)';
                } else {
                    badge.textContent = 'Live';
                    badge.style.background = '';
                    badge.style.color = '';
                    badge.style.border = '';
                }
                badge.style.opacity = '1';
            } else {
                console.error('API error:', data.error);
            }
        } catch (err) {
            console.error('Fetch error:', err);
            document.getElementById('liveBadge').style.opacity = '0.4';
        }
    }

    symbolSelect.addEventListener('change', () => {
        expirySelect.innerHTML = '<option value="">Loading…</option>';
        chainBody.innerHTML = '<tr><td colspan="15" style="text-align:center;padding:2.5rem;color:var(--muted)">Loading…</td></tr>';
        fetchData();
    });

    expirySelect.addEventListener('change', () => {
        chainBody.innerHTML = '<tr><td colspan="15" style="text-align:center;padding:2.5rem;color:var(--muted)">Loading…</td></tr>';
        fetchData();
    });

    fetchData();
    setInterval(fetchData, 5000);
})();
