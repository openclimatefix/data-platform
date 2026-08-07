window.formatEnergyValue = function(value) {
    if (value === null || value === undefined || isNaN(value)) return 'N/A';
    var num = Number(value);
    if (num >= 1000000000000) return (num / 1000000000000).toFixed(1) + ' TW';
    if (num >= 1000000000) return (num / 1000000000).toFixed(1) + ' GW';
    if (num >= 1000000) return (num / 1000000).toFixed(1) + ' MW';
    if (num >= 1000) return (num / 1000).toFixed(0) + ' kW';
    return num.toFixed(0) + ' W';
};

window.getCssVar = function(name, fallback) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
};

window.chartTheme = function() {
    var fallbackColors = ['#7a9374', '#1095c1', '#f2a900', '#e07a5f', '#8e8e93'];
    return {
        colors: fallbackColors.map(function(fallback, i) {
            return window.getCssVar('--chart-color-' + i, fallback);
        }),
        grid: window.getCssVar('--nc-bg-3', '#ebebeb'),
        text: window.getCssVar('--nc-text-2', '#8e8e93'),
        textPrimary: window.getCssVar('--nc-text-1', '#1c1c1e'),
        panelBg: window.getCssVar('--nc-bg-2', '#ffffff')
    };
};

// Replaces the copy-pasted dispose -> init -> ResizeObserver chain that used to live in
// every chart's inline <script>. Chart instances are kept on the element itself so
// repeated mounts (e.g. after an HTMX swap) don't need a chart-specific global.
window.mountChart = function(el, buildOption) {
    if (el.__chart) {
        el.__chart.dispose();
    }

    var chart = echarts.init(el);
    el.__chart = chart;
    chart.setOption(buildOption());

    var ro = new ResizeObserver(function() {
        if (chart && el.clientWidth > 0 && el.clientHeight > 0) {
            chart.resize();
        }
    });
    ro.observe(el);

    return chart;
};

document.addEventListener('htmx:configRequest', function(evt) {
    var uuid = (typeof crypto !== 'undefined' && crypto.randomUUID)
        ? crypto.randomUUID()
        : 'req' + Math.random().toString(36).substring(2) + Date.now().toString(36);
    evt.detail.headers['X-Request-Id'] = uuid.replace(/-/g, '');
});

// flatpickr targets are always brought into the DOM via an HTMX swap (there is no
// server-rendered-only path), so initialising them on htmx:afterSettle covers every case.
document.addEventListener('htmx:afterSettle', function(evt) {
    var root = evt.target;
    if (!root || typeof root.querySelector !== 'function') return;

    var rangeEl = root.querySelector('#time_window');
    if (rangeEl && !rangeEl._flatpickr) {
        // Range mode writes just the first date to the input until a second date is picked,
        // which is a validly non-empty (so `required` doesn't catch it) but incomplete value.
        var checkRangeValidity = function(selectedDates) {
            rangeEl.setCustomValidity(
                selectedDates.length === 2 ? '' : 'Please select both a start and end date.'
            );
        };

        var rangePicker = flatpickr(rangeEl, {
            mode: 'range',
            enableTime: true,
            time_24hr: true,
            dateFormat: 'Y-m-d H:i',
            onClose: checkRangeValidity
        });
        checkRangeValidity(rangePicker.selectedDates);
    }

    var singleEl = root.querySelector('#valid_from_utc');
    if (singleEl && !singleEl._flatpickr) {
        flatpickr(singleEl, {
            enableTime: true,
            time_24hr: true,
            dateFormat: 'Y-m-d H:i',
            defaultDate: new Date()
        });
    }
});
