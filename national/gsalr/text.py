import math
import re

slugify = lambda x: re.sub('[-\s]+', '-', re.sub('[^\w\s-]', '', x.strip())).lower()

def intcomma(orig):
    """
    Converts an integer to a string containing commas every three digits.
    For example, 3000 becomes '3,000' and 45000 becomes '45,000'.
    """
    new = re.sub("^(-?\d+)(\d{3})", '\g<1>,\g<2>', orig)
    if orig == new:
        return new
    else:
        return intcomma(new)

def clean_address(addr):
    """
    Given an address or intersection string, normalizes it to look pretty.
    >>> clean_address('123 MAIN')
    '123 Main'
    >>> clean_address('123 MAIN ST')
    '123 Main St.'
    >>> clean_address('123 MAIN ST S')
    '123 Main St. S.'
    >>> clean_address('123 AVENUE A')
    '123 Avenue A'
    >>> clean_address('2 N ST LAWRENCE PKWY')
    '2 N. St. Lawrence Pkwy.'
    >>> clean_address('123 NORTH AVENUE') # Don't abbreviate 'AVENUE'
    '123 North Avenue'
    >>> clean_address('123 N. Main St.')
    '123 N. Main St.'
    >>> clean_address('  123  N  WABASH  AVE   ')
    '123 N. Wabash Ave.'
    >>> clean_address('123 MAIN ST SW')
    '123 Main St. S.W.'
    >>> clean_address('123 MAIN ST NE')
    '123 Main St. N.E.'
    >>> clean_address('123 NEW YORK ST NE') # Don't punctuate 'NEW' (which contains 'NE')
    '123 New York St. N.E.'
    >>> clean_address('123 MAIN St Ne')
    '123 Main St. N.E.'
    >>> clean_address('123 MAIN St n.e.')
    '123 Main St. N.E.'
    >>> clean_address('1200 BLOCK OF WESTERN')
    '1200 block of Western'
    >>> clean_address('12XX BLOCK OF WESTERN')
    '1200 block of Western'
    >>> clean_address('XX BLOCK OF WESTERN')
    '0 block of Western'
    # The word "and" does not get capitalized.
    >>> clean_address('HAMILTON ST AND CONGRESS')
    'Hamilton St. and Congress'
    >>> clean_address('Hamilton St. And Congress')
    'Hamilton St. and Congress'
    >>> clean_address('123 ANDerson Street')
    '123 Anderson Street'
    """
    addr = smart_title(addr, ['XX', 'and'])
    addr = re.sub(r'\b(Ave|Blvd|Bvd|Cir|Ct|Dr|Ln|Pkwy|Pl|Plz|Pt|Pts|Rd|Rte|Sq|Sqs|St|Sts|Ter|Terr|Trl|Wy|N|S|E|W)(?!\.)\b', r'\1.', addr)

    # Take care of NE/NW/SE/SW.
    addr = re.sub(r'\b([NSns])\.?([EWew])\b\.?', lambda m: ('%s.%s.' % m.groups()).upper(), addr)

    addr = re.sub(r'\s\s+', ' ', addr).strip()

    # Special-case "block of" and "block" if they're after a number -- they
    # shouldn't be capitalized.
    addr = re.sub(r'(\w+) Block Of', lambda m: '%s block of' % number_to_block(m.group(1)), addr)

    return addr

def number_to_block(number, block_number=0):
    """
    Given an address number, normalizes it to the block number.
    >>> number_to_block(1)
    '0'
    >>> number_to_block(10)
    '0'
    >>> number_to_block(100)
    '100'
    >>> number_to_block(5)
    '0'
    >>> number_to_block(53)
    '0'
    >>> number_to_block(153)
    '100'
    >>> number_to_block(1000)
    '1000'
    >>> number_to_block(1030)
    '1000'
    >>> number_to_block(1359)
    '1300'
    >>> number_to_block(13593)
    '13500'
    >>> number_to_block('01')
    '0'
    >>> number_to_block('00')
    '0'
    >>> number_to_block('foo')
    'foo'
    >>> number_to_block('3xx')
    '300'
    >>> number_to_block('3XX')
    '300'
    >>> number_to_block('3pp')
    '3pp'
    >>> number_to_block('XX')
    '0'
    >>> number_to_block('X')
    'X'
    block_number lets you customize the "XX" of "3XX block".
    >>> number_to_block(234, 99)
    '299'
    >>> number_to_block(12345, 99)
    '12399'
    """
    number = re.sub('(?i)xx', '00', str(number))
    try:
        number = int(number)
    except (TypeError, ValueError):
        return number
    return str(int(math.floor(number / 100.0)) * 100 + block_number)

def address_to_block(addr):
    """
    Given an address string, normalizes it to the 100 block level.
    >>> address_to_block('1 N. Main Street')
    '0 block of N. Main Street'
    >>> address_to_block('10 N. Main Street')
    '0 block of N. Main Street'
    >>> address_to_block('123 Main Street')
    '100 block of Main Street'
    >>> address_to_block('123 MAIN STREET')
    '100 block of MAIN STREET'
    >>> address_to_block('4523 Main Street')
    '4500 block of Main Street'
    >>> address_to_block('  123 Main Street')
    '100 block of Main Street'
    >>> address_to_block('1245 Main Street')
    '1200 block of Main Street'
    """
    return re.sub(r'^\s*(\d+) ', lambda m: '%s block of ' % number_to_block(m.group(1)), addr)

def smart_title(s, exceptions=None):
    r"""
    Like .title(), but smarter.
    >>> smart_title('hello THERE')
    'Hello There'
    >>> smart_title('128th street')
    '128th Street'
    >>> smart_title('"what the heck," he said. "let\'s go to the zoo."')
    '"What The Heck," He Said. "Let\'s Go To The Zoo."'
    >>> smart_title('')
    ''
    >>> smart_title('a')
    'A'
    >>> smart_title('(this is a parenthetical.)')
    '(This Is A Parenthetical.)'
    >>> smart_title('non-functional')
    'Non-Functional'
    >>> smart_title("BILL'S HOUSE OF WAX LIPS LLC", ["of", "LLC"])
    "Bill's House of Wax Lips LLC"
    >>> smart_title("The C.I.A.", ["C.I.A."])
    'The C.I.A.'
    >>> smart_title('bill/fred')
    'Bill/Fred'
    """
    result = re.sub(r"(?<=[\s\"\(/-])(\w)", lambda m: m.group(1).upper(), s.lower())
    if result:
        result = result[0].upper() + result[1:]

    # Handle the exceptions.
    if exceptions is not None:
        for e in exceptions:
            pat = re.escape(e)
            if re.search("^\w", pat):
                pat = r"\b%s" % pat
            if re.search("\w$", pat):
                pat = r"%s\b" % pat
            pat = r"(?i)%s" % pat
            result = re.sub(pat, e, result)

    return result

def sentence_case(text):
    """
    Converts the text to all lower-case, with the first character in the string
    being capitalized. This is a poor man's sentence case.
    Calling code should assume that we'll improve this algorithm over time to
    actually capitalize words after periods, etc.
    >>> sentence_case(None)
    >>> sentence_case('')
    ''
    >>> sentence_case('this is a test')
    'This is a test'
    >>> sentence_case('This is a test')
    'This is a test'
    >>> sentence_case('tHIS test')
    'This test'
    >>> sentence_case('Two sentences here. Yeah.')
    'Two sentences here. yeah.'
    """
    if not text:
        return text
    return text[0].upper() + text[1:].lower()

def smart_excerpt(text, highlighted_text):
    """
    Returns a short excerpt of the given text with `highlighted_text`
    guaranteed to be in the middle.
    """
    m = re.search('(?:\w+\W+){0,15}%s(?:\W+\w+){0,15}' % highlighted_text, text)
    if not m:
        raise ValueError('Value not found in text')
    excerpt = m.group()
    elipsis_start = not text.startswith(excerpt)
    elipsis_end = not text.endswith(excerpt)
    if elipsis_start:
        excerpt = '...' + excerpt
    if elipsis_end:
        excerpt += '...'
    return excerpt

def hard_wrap(text, size):
    """
    >>> hard_wrap('', 4)
    ''
    >>> hard_wrap('1', 4)
    '1'
    >>> hard_wrap('123', 4)
    '123'
    >>> hard_wrap('1234', 4)
    '1234'
    >>> hard_wrap('12345', 4)
    '1234\\n5'
    >>> hard_wrap('1 12 123 1234 12345 123456789', 4)
    '1 12 123 1234 1234\\n5 1234\\n5678\\n9'
    """
    return re.sub(r'(\S{%s})((?=\S))' % size, r'\1\n\2', text)

_truncate_re = re.compile(r'(?:^\s+)?\S+\s*')

def truncate_words(s, n, end_text='...'):
    """
    White-space preserving truncate function.
    end_text is only appended if the text was actually truncated.
    """
    if n < 1:
        return u''
    bits = []
    words = 0
    it = _truncate_re.finditer(s)
    while words < n:
        try:
            bits.append(it.next().group())
        except StopIteration:
            break
        words += 1
    try:
        last = bits[-1].rstrip()
        bits[-1] = last
    except IndexError:
        pass
    trunc = u''.join(bits)
    if end_text and trunc != s and not last.endswith(end_text):
        trunc += end_text
    return trunc

if __name__ == "__main__":
    import doctest
    doctest.testmod()
