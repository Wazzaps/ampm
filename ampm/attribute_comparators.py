import datetime
import fnmatch
import re
import semver


class Comparator:
    @staticmethod
    def filter(param: str, a: str) -> bool:
        raise NotImplementedError()

    @staticmethod
    def compare(param: str, a: str, b: str) -> int:
        raise NotImplementedError()

    @staticmethod
    def help() -> str:
        raise NotImplementedError()


class NumberComparator(Comparator):
    @staticmethod
    def filter(param: str, a: str) -> bool:
        try:
            float(a)
            return True
        except ValueError:
            return False

    @staticmethod
    def compare(param, a: str, b: str) -> int:
        if param == 'biggest':
            return float(b) - float(a)
        elif param == 'smallest':
            return float(a) - float(b)
        else:
            raise ValueError('Invalid comparator parameter: ' + NumberComparator.help())

    @staticmethod
    def help():
        return "Compares numbers, e.g. @num:biggest or @num:smallest"


class DateComparator(Comparator):
    @staticmethod
    def filter(param: str, a: str) -> bool:
        try:
            datetime.datetime.fromisoformat(a)
            return True
        except ValueError:
            return False

    @staticmethod
    def compare(param, a: str, b: str) -> int:
        a = datetime.datetime.fromisoformat(a)
        b = datetime.datetime.fromisoformat(b)

        if a > b:
            result = -1
        elif a < b:
            result = 1
        else:
            result = 0

        if param == 'latest':
            return result
        elif param == 'earliest':
            return -result
        else:
            raise ValueError('Invalid comparator parameter: ' + DateComparator.help())

    @staticmethod
    def help():
        return "Compares dates, e.g. @date:latest or @date:earliest"


class SemverComparator(Comparator):
    @staticmethod
    def filter(param: str, a: str) -> bool:
        sort_order, _, is_prerelease = param.partition(',')
        try:
            ver = semver.VersionInfo.parse(a.lstrip('v'))
            if is_prerelease != 'prerelease' and ver.prerelease is not None:
                return False
            elif sort_order.startswith('^'):
                low_ver = semver.VersionInfo.parse(sort_order[1:])
                high_ver = low_ver.bump_major()
                return low_ver <= ver < high_ver
            elif sort_order.startswith('~'):
                low_ver = semver.VersionInfo.parse(sort_order[1:])
                high_ver = low_ver.bump_minor()
                return low_ver <= ver < high_ver
            elif sort_order.startswith('>'):
                low_ver = semver.VersionInfo.parse(sort_order[1:])
                return ver > low_ver
            elif sort_order.startswith('<'):
                high_ver = semver.VersionInfo.parse(sort_order[1:])
                return ver < high_ver
            else:
                return True
        except ValueError:
            return False

    @staticmethod
    def compare(param, a: str, b: str) -> int:
        sort_order, _, is_stable = param.partition(',')
        result = semver.VersionInfo.parse(a.lstrip('v')).compare(semver.VersionInfo.parse(b.lstrip('v')))

        if sort_order == 'newest' or (sort_order and sort_order[0] in '^~><'):
            return -result
        elif sort_order == 'oldest':
            return result
        else:
            raise ValueError('Invalid comparator parameter: ' + SemverComparator.help())

    @staticmethod
    def help():
        return "Compares semver versions, e.g. @semver:newest or @semver:oldest or @semver:'^1.1.0' or " \
               "@semver:'~1.1.0' or @semver:'>1.1.0' or @semver:'<1.1.0'. \n" \
               "You can add `,prerelease` (like so: `@semver:newest,unstable`) to allow prerelease versions " \
               "(like `1.2.3-alpha`)\n" \
               "Only accepts semver versions, e.g. v1.2.3 or 1.2.3 or 1.2.3-alpha or 1.2.3-alpha+somestring"


class GlobComparator(Comparator):
    @staticmethod
    def filter(param: str, a: str) -> bool:
        return fnmatch.fnmatch(a, param)

    @staticmethod
    def compare(param, a: str, b: str) -> int:
        return 0

    @staticmethod
    def help():
        return "Filters by glob, e.g. @glob:x86* or @glob:mips??32 or @glob:armv[67]*"


class RegexComparator(Comparator):
    @staticmethod
    def filter(param: str, a: str) -> bool:
        return re.match(param, a) is not None

    @staticmethod
    def compare(param, a: str, b: str) -> int:
        return 0

    @staticmethod
    def help():
        return "Filters by regex, e.g. @regex:i386|x86_64 or @regex:^v1.[01234]$"


COMPARATORS = {
    '@num': NumberComparator,
    '@date': DateComparator,
    '@semver': SemverComparator,
    '@glob': GlobComparator,
    '@regex': RegexComparator,
}
