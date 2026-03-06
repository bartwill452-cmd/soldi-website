(function() {
  'use strict';

  // Generate or retrieve session ID
  var sid = sessionStorage.getItem('soldi_sid');
  if (!sid) {
    sid = Math.random().toString(36).substr(2) + Date.now().toString(36);
    sessionStorage.setItem('soldi_sid', sid);
  }

  var page = location.pathname || '/';
  var endpoint = '/api/analytics';

  // Send beacon (uses sendBeacon for reliability, falls back to fetch)
  function send(path, data) {
    data.sid = sid;
    var body = JSON.stringify(data);
    if (navigator.sendBeacon) {
      navigator.sendBeacon(endpoint + path, new Blob([body], { type: 'application/json' }));
    } else {
      fetch(endpoint + path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body,
        keepalive: true
      }).catch(function() {});
    }
  }

  // Parse UTM params
  var params = new URLSearchParams(location.search);
  var utm = {};
  ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content'].forEach(function(k) {
    var v = params.get(k);
    if (v) utm[k] = v;
  });

  // Record page view on load
  send('/pageview', {
    page: page,
    referrer: document.referrer || 'direct',
    ua: navigator.userAgent,
    screen: screen.width + 'x' + screen.height,
    utm_source: utm.utm_source || '',
    utm_medium: utm.utm_medium || '',
    utm_campaign: utm.utm_campaign || ''
  });

  // Heartbeat every 30 seconds
  setInterval(function() {
    send('/heartbeat', { page: page });
  }, 30000);

  // Expose global tracking function (replaces GA4 trackEvent)
  window.soldiTrack = function(event, props) {
    send('/event', {
      event: event,
      props: props || {},
      page: page
    });
  };

  // Backward compat alias — existing trackEvent() calls route here
  window.trackEvent = window.soldiTrack;

  // Re-send heartbeat when tab becomes visible again
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'visible') {
      send('/heartbeat', { page: page });
    }
  });
})();
