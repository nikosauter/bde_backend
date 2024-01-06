// Use the /post endpoint to send a new post
function createPost() {
    const content = document.getElementById("content").value;
    fetch("/post", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({ post_content: content }),
    })
    .then((response) => response.json())
    .then((data) => {
        console.log(data.message);
    })
    .catch((error) => {
        console.error("Error:", error);
    });
}

// Display retrieved information on trending topics in form of an unordered list
function renderTrendingList(data, elementId, labelId) {
    const trendingList = document.getElementById(elementId);
    while (trendingList.firstChild) {
        trendingList.removeChild(trendingList.firstChild);
    }
    if (data.length === 0) {
        const trendingListLabel = document.getElementById(labelId);
        trendingListLabel.textContent = "No trending topics on this day"
    } else {
        data.forEach(topic => {
            const listItem = document.createElement("li");
            listItem.textContent = `${topic.hashtag}, Counter: ${topic.counter}`;
            trendingList.appendChild(listItem);
        });
    }
}

// Fetch and display trending topics on a specific day
function selectDate() {
    const content = document.getElementById("selected_date").value;
    const params = { selected_date: content };
    const urlParams = new URLSearchParams(params);
    const url = '/discover_trending?' + urlParams.toString();
    fetch(url)
    .then(response => response.json())
    .then(data => {
        renderTrendingList(data, "trending-list-of-date", "trending-list-of-date-label");
    })
    .catch(error => {
        console.error("Error:", error);
    });
}

window.addEventListener('DOMContentLoaded', (event) => {
    const results = window.results;
    renderTrendingList(results, "trending-list", "trending-list-label");
});
