package com.example.urldownloader
import android.os.Bundle
import android.widget.Button
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import java.io.IOException
class MainActivity : AppCompatActivity() {
    private lateinit var button: Button
    private lateinit var textView: TextView
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        title = "KotlinApp"
        textView = findViewById(R.id.textView)
        button = findViewById(R.id.btnParseHTML)
        button.setOnClickListener {
            getHtmlFromWeb()
        }
    }
    private fun getHtmlFromWeb() {
        Thread {
            val stringBuilder = StringBuilder()
            try {
                val doc: Document = Jsoup.connect("https://www.mytfgworld.com/").get()
                val title: String = doc.title()
                val links: Elements = doc.select("a[href]")
                stringBuilder.append(title).append("\n")
                for (link in links) {
                    stringBuilder.append("\n").append("Link :").append(link.attr("href"))
                        .append("\n").append("Text : ").append(link.text())
                }
            } catch (e: IOException) {
                stringBuilder.append("Error : ").append(e.message).append("\n")
            }
            runOnUiThread { textView.text = stringBuilder.toString() }
        }.start()
    }
}