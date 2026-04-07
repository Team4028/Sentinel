window.sha256 = async function (str) {
    const strBuf = new TextEncoder().encode(str);
    const hashBuf = await window.crypto.subtle.digest('SHA-256', strBuf);
    const hashArr = Array.from(new Uint8Array(hashBuf));
    return hashArr.map(b => ('00' + b.toString(16)).slice(-2)).join('');
}