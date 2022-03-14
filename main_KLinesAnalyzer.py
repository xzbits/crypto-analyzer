from src.KLinesAnalyzer import KLinesAnalyzer
import src.Config as Config


if __name__ == "__main__":
    example = KLinesAnalyzer("MANA", "USDT", Config, "15 Jan 2022", "15 Feb 2022")
    example.save_notable_period(option=2)
