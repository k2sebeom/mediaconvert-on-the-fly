const inp = document.getElementById("file-element");
const btn = document.getElementById("upload-btn");
const urlEl = document.getElementById("url-element");
const videoEl = document.getElementById("video-element");

const ENDPOINT = "http://ec2-54-180-94-253.ap-northeast-2.compute.amazonaws.com/";

let fileName = "";
let monitorId = "";

function monitorUrl() {
    monitorId = setInterval(async () => {
        let resp = await fetch(ENDPOINT + "url/?file_name=" + fileName);
        let result = await resp.json();
        if (result.url != null) {
            urlEl.innerText = result.url;
            let hls = new Hls();
            hls.loadSource(result.url);
            hls.attachMedia(videoEl);
            clearInterval(monitorId);
        }
        else {
            urlEl.innerText = "Preparing video...";
        }
    }, 1000);
}

btn.addEventListener("click", async (e) => {
    btn.disabled = true;
    let data = new FormData();
    const file = inp.files[0];
    data.append("file", file);
    let resp = await fetch(ENDPOINT + "file", {method: 'post', body: data});
    alert("File is uploaded");
    inp.value = "";
    btn.disabled = false;
    fileName = file.name;
    videoEl.src = "";
    monitorUrl();
})

