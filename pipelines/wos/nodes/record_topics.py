def split_topic(topic: str):
    num_topic, name_topic = topic.split(" ", 1)
    return {"num_topics": num_topic, "name_topics": name_topic}
