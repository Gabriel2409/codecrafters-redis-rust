use nom::{
    bytes::complete::{tag, take},
    character::complete,
    multi::count,
    IResult,
};

#[derive(Debug)]
pub struct RedisSentence {
    pub nb_words: usize,
    pub words: Vec<String>,
}

/// Redis separates information with \r\n
fn parse_end_line(input: &str) -> IResult<&str, &str> {
    tag("\r\n")(input)
}

/// Each redis commands start with the nb of words
fn parse_nb_words(input: &str) -> IResult<&str, usize> {
    let (input, _) = tag("*")(input)?;
    let (input, nb_words) = complete::u32(input)?;
    let (input, _) = parse_end_line(input)?;
    Ok((input, nb_words as usize))
}

fn parse_word_length(input: &str) -> IResult<&str, usize> {
    let (input, _) = tag("$")(input)?;
    let (input, word_length) = complete::u32(input)?;
    let (input, _) = parse_end_line(input)?;
    Ok((input, word_length as usize))
}

fn parse_fixed_length(input: &str, length: usize) -> IResult<&str, &str> {
    let (input, word) = take(length)(input)?;
    let (input, _) = parse_end_line(input)?;
    Ok((input, word))
}

fn parse_word(input: &str) -> IResult<&str, &str> {
    let (input, word_length) = parse_word_length(input)?;
    let (input, word) = parse_fixed_length(input, word_length)?;

    Ok((input, word))
}

pub fn parse_sentence(input: &str) -> IResult<&str, RedisSentence> {
    let (input, nb_words) = parse_nb_words(input)?;

    let (input, words) = count(parse_word, nb_words)(input)?;
    let sentence = RedisSentence {
        nb_words,
        words: words.into_iter().map(|w| w.to_owned()).collect(),
    };
    Ok((input, sentence))
}

#[cfg(test)]
mod tests {
    use nom::Finish;

    use super::*;
    use crate::Result;

    #[test]
    fn test_parse_statement() -> Result<()> {
        let input = "*2\r\n$4\r\nEcho\r\n$7\r\nbonjour\r\n";
        let (input, sentence) = parse_sentence(input).finish()?;
        assert_eq!(sentence.nb_words, 2);
        assert_eq!(sentence.words, vec!["Echo", "bonjour"]);
        assert_eq!(input, "");
        Ok(())
    }
}
