from producer import clean_text, parse_item
assert clean_text("<p>Hi &amp; bye<br>there</p><a href='x'>link</a>") == "Hi & bye there link"
assert clean_text(None) == ""
s = parse_item(1, {"type": "story", "title": "Apple <b>news</b>", "time": 5})
assert s["id"] == "1" and s["text"] == "Apple news" and s["source"] == "hackernews", s
assert s["type"] == "story" and s["timestamp"] == 5 and s["collected_at"].endswith("+00:00"), s
assert parse_item(2, {"type": "comment", "text": "<p>Google &#x27;ok&#x27;"})["text"] == "Google 'ok'"
for bad in (None, {"deleted": True}, {"dead": True, "type": "comment", "text": "x"},
            {"type": "job", "title": "x"}, {"type": "comment", "text": "<p></p>"}, {"type": "poll"}):
    assert parse_item(3, bad) is None, bad
print("offline tests passed")
