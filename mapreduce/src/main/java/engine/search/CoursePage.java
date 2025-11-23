package engine.search;

public class CoursePage {
    private String webUrl;
    private String content; 

    public CoursePage(String webString, String contentText){
        this.webUrl = webString;
        this.content = contentText;
    }

    public String getContent() {
        return content;
    }

    public String getWebUrl() {
        return webUrl;
    }

    @Override
    public String toString() {
        return webUrl + " & " + content;
    }
}
