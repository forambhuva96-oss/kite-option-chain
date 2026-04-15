(() => {
    const symbolSelect = document.getElementById('symbolSelect');
    const spotPriceEl  = document.getElementById('spotPrice');
    const expiryLabel  = document.getElementById('expiryLabel');
    const chainBody    = document.getElementById('chainBody');

    const fmt  = (n) => n != null ? new Intl.NumberFormat('en-IN').format(n) : '—';
    const fmtP = (n, d = 2) => (n != null && n !== undefined) ? Number(n).toFixed(d) : '—';
    const chSgn = (v) => v > 0 ? '+' + fmtP(v) : fmtP(v);

    /* ── find top-2 values in ATM + OTM rows only ── */
    function findMaxes(chain, atm_strike) {
        // Each field stores [1st, 2nd] max values
        const mx = {
            CE: { volume:[-Infinity,-Infinity], oi:[-Infinity,-Infinity], oi_change:[-Infinity,-Infinity] },
            PE: { volume:[-Infinity,-Infinity], oi:[-Infinity,-Infinity], oi_change:[-Infinity,-Infinity] },
        };
        function update(arr, val) {
            if (val > arr[0])      { arr[1] = arr[0]; arr[0] = val; }
            else if (val > arr[1]) { arr[1] = val; }
        }
        chain.forEach(r => {
            // CE: ATM and OTM calls → strike >= atm
            if (r.strike >= atm_strike && r.CE) {
                const d = r.CE;
                update(mx.CE.volume,    d.volume);
                update(mx.CE.oi,        d.oi);
                update(mx.CE.oi_change, Math.abs(d.oi_change));
            }
            // PE: ATM and OTM puts → strike <= atm
            if (r.strike <= atm_strike && r.PE) {
                const d = r.PE;
                update(mx.PE.volume,    d.volume);
                update(mx.PE.oi,        d.oi);
                update(mx.PE.oi_change, Math.abs(d.oi_change));
            }
        });
        return mx;
    }


    function td(content, classes = []) {
        const cls = classes.filter(Boolean).join(' ');
        return `<td${cls ? ` class="${cls}"` : ''}>${content}</td>`;
    }

    function renderChain(data) {
        spotPriceEl.textContent = '₹' + fmt(data.spot_price);
        expiryLabel.textContent = `Expiry: ${data.expiry}`;
        const mx = findMaxes(data.chain, data.atm_strike);

        let html = '';
        data.chain.forEach(row => {
            const isAtm = row.strike === data.atm_strike;
            const ceItm = row.strike < data.atm_strike;
            const peItm = row.strike > data.atm_strike;
            const ce = row.CE, pe = row.PE;

            const hlCls = (side, field, d) => {
                if (!d) return null;
                const arr  = mx[side][field];           // [1st, 2nd]
                const val  = field === 'oi_change' ? Math.abs(d[field]) : d[field];
                if (val === arr[0] && arr[0] !== -Infinity) return side === 'CE' ? 'hl-ce' : 'hl-pe';
                if (val === arr[1] && arr[1] !== -Infinity) return 'hl-2nd';
                return null;
            };

            html += `<tr${isAtm ? ' class="atm-row"' : ''}>`;

            // ── CALLS: Greeks | OI | ChgOI | Volume | LTP ──
            html += td(ce?.iv    != null ? fmtP(ce.iv)     : '—', [ceItm?'ce-itm':null,'greek']);
            html += td(ce?.delta != null ? fmtP(ce.delta,3): '—', [ceItm?'ce-itm':null,'greek']);
            html += td(ce?.theta != null ? fmtP(ce.theta)  : '—', [ceItm?'ce-itm':null,'greek']);
            html += td(ce ? fmt(ce.oi) : '—',        [ceItm?'ce-itm':null, hlCls('CE','oi',ce)]);
            html += td(ce ? chSgn(ce.oi_change):'—', [ceItm?'ce-itm':null, hlCls('CE','oi_change',ce),
                                                        ce&&ce.oi_change>=0?'oi-change up':'oi-change dn']);
            html += td(ce ? fmt(ce.volume) : '—',    [ceItm?'ce-itm':null, hlCls('CE','volume',ce)]);
            html += td(ce ? fmtP(ce.ltp) : '—',     [ceItm?'ce-itm':null,'ltp']);

            // ── STRIKE ──
            html += `<td class="strike-cell">${fmt(row.strike)}</td>`;

            // ── PUTS: LTP | Volume | ChgOI | OI | Greeks ──
            html += td(pe ? fmtP(pe.ltp) : '—',     [peItm?'pe-itm':null,'ltp']);
            html += td(pe ? fmt(pe.volume) : '—',    [peItm?'pe-itm':null, hlCls('PE','volume',pe)]);
            html += td(pe ? chSgn(pe.oi_change):'—', [peItm?'pe-itm':null, hlCls('PE','oi_change',pe),
                                                        pe&&pe.oi_change>=0?'oi-change up':'oi-change dn']);
            html += td(pe ? fmt(pe.oi) : '—',        [peItm?'pe-itm':null, hlCls('PE','oi',pe)]);
            html += td(pe?.iv    != null ? fmtP(pe.iv)     : '—', [peItm?'pe-itm':null,'greek']);
            html += td(pe?.delta != null ? fmtP(pe.delta,3): '—', [peItm?'pe-itm':null,'greek']);
            html += td(pe?.theta != null ? fmtP(pe.theta)  : '—', [peItm?'pe-itm':null,'greek']);

            html += '</tr>';
        });

        chainBody.innerHTML = html;
    }

    async function fetchData() {
        const symbol = symbolSelect.value;
        try {
            const res  = await fetch(`/api/option-chain?symbol=${symbol}`);
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
        chainBody.innerHTML = '<tr><td colspan="15" style="text-align:center;padding:2.5rem;color:var(--muted)">Loading…</td></tr>';
        fetchData();
    });

    fetchData();
    setInterval(fetchData, 5000);
})();
