from src.KLinesAnalyzer import KLinesAnalyzer
import config


if __name__ == "__main__":
    example = KLinesAnalyzer("MANA", "USDT", config, "15 Jan 2022", "15 Feb 2022")
    example.save_increase_period()
