package ru.mail.polis.service.shakhmin;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

final class VNode {

    private final long token;

    @NotNull
    private String address;

    VNode(final long token,
          @NotNull final String address) {
        this.token = token;
        this.address = address;
    }

    long getToken() {
        return token;
    }

    @NotNull
    String getAddress() {
        return address;
    }

    void setAddress(@NotNull String address) {
        this.address = address;
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, address);
    }

    @Override
    public boolean equals(@NotNull final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof VNode)) {
            return false;
        }
        final var vNode = (VNode)o;
        return vNode.token == token && vNode.address.equals(address);
    }
}
