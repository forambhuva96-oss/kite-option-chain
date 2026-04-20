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

    function findMaxes(chain) {
        const mx = {
            CE: { vol: -Infinity, oi: -Infinity, oi_chg: -Infinity },
            PE: { vol: -Infinity, oi: -Infinity, oi_chg: -Infinity }
        };
        chain.forEach(r => {
            if (r.CE) {
                if (r.CE.volume != null && r.CE.volume > mx.CE.vol) mx.CE.vol = r.CE.volume;
                if (r.CE.oi != null && r.CE.oi > mx.CE.oi) mx.CE.oi = r.CE.oi;
                if (r.CE.intraday_oi_change != null && r.CE.intraday_oi_change > mx.CE.oi_chg) mx.CE.oi_chg = r.CE.intraday_oi_change;
            }
            if (r.PE) {
                if (r.PE.volume != null && r.PE.volume > mx.PE.vol) mx.PE.vol = r.PE.volume;
                if (r.PE.oi != null && r.PE.oi > mx.PE.oi) mx.PE.oi = r.PE.oi;
                if (r.PE.intraday_oi_change != null && r.PE.intraday_oi_change > mx.PE.oi_chg) mx.PE.oi_chg = r.PE.intraday_oi_change;
            }
        });
        return mx;
    }

    function td(content, classes = []) {
        const cls = classes.filter(Boolean).join(' ');
        return `<td${cls ? ` class="${cls}"` : ''}>${content}</td>`;
    }

    function oiChangeTd(momentum, intraday, side, baseState, chgCls, chgTag) {
        const upDn = v => v == null ? '' : (v >= 0 ? 'oi-up' : 'oi-dn');

        const momentumHtml = `<span class="oi-overnight ${upDn(momentum)}">${chSgn(momentum)}</span>`;
        const intradayHtml  = intraday != null
            ? `<span class="oi-intraday ${upDn(intraday)}" title="Intraday vs Morning">${chSgn(intraday)}</span>`
            : '';

        return `<td class="${[baseState, chgCls, 'oi-chg-cell'].filter(Boolean).join(' ')}">
            <div style="display:flex; justify-content:flex-end; align-items:center;">
                <div class="oi-chg-wrap" style="margin-right:2px">${momentumHtml}${intradayHtml}</div>
                ${chgTag}
            </div>
        </td>`;
    }

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

        const mx = findMaxes(data.chain);
        let html = '';
        data.chain.forEach(row => {
            const isAtm = Number(row.strike) === Number(data.atm_strike);
            const ceItm = Number(row.strike) < Number(data.atm_strike);
            const peItm = Number(row.strike) > Number(data.atm_strike);
            
            const ceState = ceItm ? 'itm' : 'otm';
            const peState = peItm ? 'itm' : 'otm';
            const ce = row.CE, pe = row.PE;
            
            // Evaluators
            const isCeVolMax = ce && ce.volume != null && ce.volume === mx.CE.vol && mx.CE.vol > -Infinity;
            const isCeOiMax = ce && ce.oi != null && ce.oi === mx.CE.oi && mx.CE.oi > -Infinity;
            const isCeChgMax = ce && ce.intraday_oi_change != null && ce.intraday_oi_change === mx.CE.oi_chg && mx.CE.oi_chg > -Infinity;
            
            const isPeVolMax = pe && pe.volume != null && pe.volume === mx.PE.vol && mx.PE.vol > -Infinity;
            const isPeOiMax = pe && pe.oi != null && pe.oi === mx.PE.oi && mx.PE.oi > -Infinity;
            const isPeChgMax = pe && pe.intraday_oi_change != null && pe.intraday_oi_change === mx.PE.oi_chg && mx.PE.oi_chg > -Infinity;

            const formatAction = (str) => {
                if(!str || str==="NO TRADE") return `<span style="color: var(--muted)">—</span>`;
                if(str.includes("BUY")) return `<span style="color: #00ff88; font-weight:bold;">${str}</span>`;
                return `<span style="color: #ff4d4d; font-weight:bold;">${str}</span>`;
            };
            const formatAlert = (str, alertFlag) => {
                if(!str || str==="No Trade") return `<span style="color: var(--muted)">Idle</span>`;
                return alertFlag ? `<span style="color: #00ff88; font-weight:bold; font-size:1.05em">★ ${str}</span>` : `<span>${str}</span>`;
            };
            const formatStrength = (str) => {
                if(!str || str==="weak") return `<span style="color: var(--muted)">Weak</span>`;
                if(str==="strong") return `<span style="color: #007bff; font-weight:bold;">Strong</span>`;
                return `<span style="color: #fde68a;">Moderate</span>`;
            }

            html += `<tr${isAtm ? ' class="atm-row"' : ''}>`;

            // CALLS ALGO: Action | Signal | Strength
            html += td(ce ? formatAction(ce.action)   : '—', [ceState,'greek']);
            html += td(ce ? formatAlert(ce.signal, ce.alert) : '—', [ceState,'greek']);
            html += td(ce ? formatStrength(ce.strength) : '—', [ceState,'greek']);
            
            // CALLS DATA: OI | Chg OI | Volume | LTP
            let ceOiCls = (!isAtm && isCeOiMax) ? 'ce-oi-max' : null;
            let ceOiTag = isCeOiMax ? ' <span class="max-tag" style="' + (isAtm?'background:#00ff88;color:#000;':'') + '">OI↑</span>' : '';
            html += td(ce ? fmt(ce.oi) + ceOiTag : '—', [ceState, ceOiCls]);
            
            let ceChgCls = (!isAtm && isCeChgMax) ? 'ce-oichg-max' : null;
            let ceChgTag = isCeChgMax ? ' <span class="max-tag" style="' + (isAtm?'background:#66ffcc;color:#000;':'') + '">ΔOI↑</span>' : '';
            html += oiChangeTd(ce?.momentum_oi_change, ce?.intraday_oi_change, 'CE', ceState, ceChgCls, ceChgTag);
            
            let ceVolCls = (!isAtm && isCeVolMax) ? 'ce-vol-max' : null;
            let ceVolTag = isCeVolMax ? ' <span class="max-tag" style="' + (isAtm?'background:#00cc66;color:#000;':'') + '">VOL↑</span>' : '';
            html += td(ce ? fmt(ce.volume) + ceVolTag : '—', [ceState, ceVolCls]);
            
            html += td(ce ? fmtP(ce.ltp)   : '—', [ceState,'ltp']);

            // STRIKE
            html += `<td class="strike-cell">${fmt(row.strike)}</td>`;

            // PUTS DATA: LTP | Volume | Chg OI | OI
            html += td(pe ? fmtP(pe.ltp)   : '—', [peState,'ltp']);
            
            let peVolCls = (!isAtm && isPeVolMax) ? 'pe-vol-max' : null;
            let peVolTag = isPeVolMax ? ' <span class="max-tag" style="' + (isAtm?'background:#ff1a1a;color:#fff;':'') + '">VOL↑</span>' : '';
            html += td(pe ? fmt(pe.volume) + peVolTag : '—', [peState, peVolCls]);

            let peChgCls = (!isAtm && isPeChgMax) ? 'pe-oichg-max' : null;
            let peChgTag = isPeChgMax ? ' <span class="max-tag" style="' + (isAtm?'background:#ff9999;color:#000;':'') + '">ΔOI↑</span>' : '';
            html += oiChangeTd(pe?.momentum_oi_change, pe?.intraday_oi_change, 'PE', peState, peChgCls, peChgTag);

            let peOiCls = (!isAtm && isPeOiMax) ? 'pe-oi-max' : null;
            let peOiTag = isPeOiMax ? ' <span class="max-tag" style="' + (isAtm?'background:#ff4d4d;color:#fff;':'') + '">OI↑</span>' : '';
            html += td(pe ? fmt(pe.oi) + peOiTag : '—', [peState, peOiCls]);
            
            // PUTS ALGO: Strength | Signal | Action
            html += td(pe ? formatStrength(pe.strength) : '—', [peState,'greek']);
            html += td(pe ? formatAlert(pe.signal, pe.alert) : '—', [peState,'greek']);
            html += td(pe ? formatAction(pe.action)   : '—', [peState,'greek']);

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
                document.getElementById('liveBadge').style.opacity = '1';
            } else {
                console.error('API error:', data.error);
                chainBody.innerHTML = `<tr><td colspan="17" style="text-align:center;padding:2.5rem;color:var(--danger)">[SYSTEM EXCEPTION]: ${data.error}<br><br><a href="/" style="color:var(--primary); text-decoration:underline;">Click Here to Authenticate Engine</a></td></tr>`;
            }
        } catch (err) {
            console.error('Fetch error:', err);
            document.getElementById('liveBadge').style.opacity = '0.4';
            chainBody.innerHTML = `<tr><td colspan="17" style="text-align:center;padding:2.5rem;color:var(--danger)">[NETWORK ERROR]: Cannot reach backend. Please wait...</td></tr>`;
        }
    }

    symbolSelect.addEventListener('change', () => {
        expirySelect.innerHTML = '<option value="">Loading…</option>';
        chainBody.innerHTML = '<tr><td colspan="17" style="text-align:center;padding:2.5rem;color:var(--muted)">Loading…</td></tr>';
        fetchData();
    });

    expirySelect.addEventListener('change', () => {
        chainBody.innerHTML = '<tr><td colspan="17" style="text-align:center;padding:2.5rem;color:var(--muted)">Loading…</td></tr>';
        fetchData();
    });

    fetchData();
    setInterval(fetchData, 8000);
})();
