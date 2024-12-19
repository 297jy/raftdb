package org.zhuanyi.jraftdb.engine.dto;

/**
 * leveldb 状态定义，在一个项目里面做统一的状态定义
 */
public class Status {

    public enum Code {
        K_OK(0),
        K_NOT_FOUND(1),
        K_CORRUPTION(2),
        K_NOT_SUPPORTED(3),
        K_INVALID_ARGUMENT(4),
        K_IO_ERROR(5),
        ;

        private final int value;

        Code(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    private final Code code;

    private final String message;

    public Status(Code code, String message) {
        this.code = code;
        this.message = message;
    }

    public boolean isOk() {
        return code == Code.K_OK;
    }

    public boolean isNotFound() {
        return code == Code.K_NOT_FOUND;
    }

    public boolean isNotSupported() {
        return code == Code.K_NOT_SUPPORTED;
    }

    public boolean isInvalidArgument() {
        return code == Code.K_INVALID_ARGUMENT;
    }

    public boolean isIOError() {
        return code == Code.K_IO_ERROR;
    }

    public boolean isCorruption() {
        return code == Code.K_CORRUPTION;
    }

    public Code getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static Status ok() {
        return new Status(Code.K_OK, null);
    }

    public static Status notFound(String msg) {
        return new Status(Code.K_NOT_FOUND, msg);
    }

    public static Status notSupported(String msg) {
        return new Status(Code.K_NOT_SUPPORTED, msg);
    }

    public static Status invalidArgument(String msg) {
        return new Status(Code.K_INVALID_ARGUMENT, msg);
    }

    public static Status ioError(String msg) {
        return new Status(Code.K_IO_ERROR, msg);
    }

    public static Status corruption(String msg) {
        return new Status(Code.K_CORRUPTION, msg);
    }
}
