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

// Chart instances are kept on the element itself so a repeated mount (e.g. after an HTMX
// swap replaces the element's container but reuses its id) disposes the old instance first.
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

// Closes the forecaster/observer picker dropdown on an outside click. Registered once here
// (rather than once per widget init) since there is only ever one #source_chips on the page.
document.addEventListener('click', function(e) {
    var badge = document.getElementById('source_badge');
    var chips = document.getElementById('source_chips');
    if (!badge || !chips) return;
    if (!e.target.closest('#source_badge') && !e.target.closest('#source_chips')) {
        chips.classList.remove('show');
    }
});

function queryAllIncludingRoot(root, selector) {
    var matches = root.matches && root.matches(selector) ? [root] : [];
    return matches.concat(Array.prototype.slice.call(root.querySelectorAll(selector)));
}

// Templates mark their chart containers with data-chart="<kind>" and data-chart-config="<id
// of the application/json config script>" instead of an inline <script> calling the mount
// function directly - the latter meant interpolating server-side ids into JS string literals.
function mountCharts(root) {
    var mounts = {
        forecast: window.mountForecastChart,
        capacity: window.mountCapacityChart,
        choropleth: window.mountChoroplethMap,
        location: window.mountLocationMap
    };

    queryAllIncludingRoot(root, '[data-chart]').forEach(function(el) {
        var mount = mounts[el.dataset.chart];
        if (mount && el.dataset.chartConfig) {
            mount(el.id, el.dataset.chartConfig);
        }
    });
}

// The forecaster/observer picker on the query form: an add/remove chip widget backed by
// hidden inputs (one per selected source) so the form submits them without further JS.
function initSelectorsWidget(form) {
    var locSearch = form.querySelector('#location_search');
    var locUuid = form.querySelector('#location_uuid');
    var sourceSearch = form.querySelector('#source_search');
    var sourceHorizon = form.querySelector('#source_horizon');
    var sourceBadge = form.querySelector('#source_badge');
    var sourceChips = form.querySelector('#source_chips');
    var fcContainer = form.querySelector('#fc_chips_container');
    var obsContainer = form.querySelector('#obs_chips_container');
    var hiddenInputs = form.querySelector('#source_hidden_inputs');
    var chipTemplate = form.querySelector('#chip_template');
    var btnAddSource = form.querySelector('#btn_add_source');

    function findOption(listId, value) {
        var list = form.querySelector('#' + listId);
        if (!list) return null;
        return Array.from(list.options).find(function(o) { return o.value === value; });
    }

    function updateLocationValidity() {
        locSearch.setCustomValidity(locUuid.value ? '' : 'Please select a valid location from the list.');
    }

    locSearch.addEventListener('input', function(e) {
        locUuid.value = '';
        var opt = findOption('location_options', e.target.value);
        if (opt) locUuid.value = opt.getAttribute('data-uuid');
        updateLocationValidity();
    });

    // Uniqueness rule: a source is identified by its (type, value) pair. A forecaster's
    // value already encodes the horizon ("name|version|horizon"), so the same forecaster
    // picked at two different horizons is treated as two distinct sources.
    function sourceExists(type, value) {
        return !!hiddenInputs.querySelector('input[data-type="' + type + '"][data-value="' + CSS.escape(value) + '"]');
    }

    function updateSourceBadge() {
        var total = hiddenInputs.children.length;
        sourceBadge.textContent = total + ' Selected';
        sourceSearch.setCustomValidity(total > 0 ? '' : 'Please select at least one forecaster or observer.');
    }

    function addSource(type, value, label) {
        if (sourceExists(type, value)) return;

        var chip = chipTemplate.content.firstElementChild.cloneNode(true);
        chip.dataset.type = type;
        chip.dataset.value = value;
        chip.querySelector('.chip-text').textContent = label;
        chip.querySelector('.chip-text').title = label;
        (type === 'forecaster' ? fcContainer : obsContainer).appendChild(chip);

        var hidden = document.createElement('input');
        hidden.type = 'hidden';
        hidden.name = type;
        hidden.value = value;
        hidden.dataset.type = type;
        hidden.dataset.value = value;
        hiddenInputs.appendChild(hidden);

        updateSourceBadge();
    }

    function removeSource(type, value) {
        var container = type === 'forecaster' ? fcContainer : obsContainer;
        var chip = container.querySelector('[data-value="' + CSS.escape(value) + '"]');
        if (chip) chip.remove();

        var hidden = hiddenInputs.querySelector('input[data-type="' + type + '"][data-value="' + CSS.escape(value) + '"]');
        if (hidden) hidden.remove();

        updateSourceBadge();
    }

    btnAddSource.addEventListener('click', function() {
        var opt = findOption('source_options', sourceSearch.value);
        if (!opt) return;

        var type = opt.dataset.type;
        var label = opt.value;

        if (type === 'forecaster') {
            var horizon = sourceHorizon.value || '0';
            addSource(type, opt.dataset.val + '|' + horizon, label.replace(' [Forecaster]', '') + ' @ ' + horizon + 'm');
        } else {
            addSource(type, opt.dataset.val, label);
        }

        sourceSearch.value = '';
    });

    // Chip-close buttons are created dynamically, so a single delegated listener on the
    // (static) chips container handles all of them.
    sourceChips.addEventListener('click', function(e) {
        var closeBtn = e.target.closest('.chip-close');
        if (!closeBtn) return;
        var chip = closeBtn.closest('.chip');
        removeSource(chip.dataset.type, chip.dataset.value);
    });

    sourceBadge.addEventListener('click', function() {
        sourceChips.classList.toggle('show');
    });

    updateLocationValidity();
    updateSourceBadge();
}

function initWidgets(root) {
    queryAllIncludingRoot(root, '#query-form').forEach(initSelectorsWidget);
}

function initFlatpickr(root) {
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
}

// flatpickr targets, chart containers and the selectors widget are all brought into the DOM
// via an HTMX swap (there is no server-rendered-only path), so this one listener covers them.
document.addEventListener('htmx:afterSettle', function(evt) {
    var root = evt.target;
    if (!root || typeof root.querySelector !== 'function') return;

    initFlatpickr(root);
    mountCharts(root);
    initWidgets(root);
});
