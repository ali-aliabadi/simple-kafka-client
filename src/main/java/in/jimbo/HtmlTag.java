package in.jimbo;


import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class HtmlTag {
    private String name;
    private String content;
    private Map<String, String> props;

    public String getName() {
        return name;
    }

    public String getContent() {
        return content;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public HtmlTag(String name, String content) {
        this.name = name;
        this.content = content;
        this.props = new HashMap<>();
    }

    public HtmlTag(String name) {
        this.name = name;
        this.content = "";
        this.props = new HashMap<>();
    }

    public HtmlTag() {
        this.name = "";
        this.content = "";
        this.props = new HashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HtmlTag htmlTag = (HtmlTag) o;
        return Objects.equals(name, htmlTag.name) &&
                Objects.equals(content, htmlTag.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, content);
    }

    @Override
    public String toString() {
        return "name : " + name + ", content : " + content + ", probs : " + props.toString();
    }
}
