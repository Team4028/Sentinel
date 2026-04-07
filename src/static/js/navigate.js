window.goBack = function () {
    var lastUrl = new URLSearchParams(window.location.search).get("last") ?? "/";
    window.location.href = lastUrl;
}

window.goTo = function (url) {
    const sepchar = url.includes("?") ? '&' : '?';
    window.location.href = `${url}${sepchar}last=${window.location.href}`;
}